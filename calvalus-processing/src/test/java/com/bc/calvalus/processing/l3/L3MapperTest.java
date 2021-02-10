package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.calvalus.processing.utils.DateLineOps;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.binding.BindingException;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.esa.snap.binning.Aggregator;
import org.esa.snap.binning.BinManager;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.DataPeriod;
import org.esa.snap.binning.MosaickingGrid;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.support.BinningContextImpl;
import org.esa.snap.binning.support.CrsGrid;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.SystemUtils;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.projection.MapProjection;
import org.junit.Ignore;
import org.junit.Test;
import org.opengis.geometry.Envelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;

import java.awt.geom.AffineTransform;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class L3MapperTest {

    @Test
    public void testExtractProcessingGraphXml() {
        final Product product = new Product("bla", "blub", 3, 3);


        final MetadataElement processingGraph = new MetadataElement("Processing_Graph");
        final MetadataAttribute metadataAttribute = new MetadataAttribute("test_attrib", ProductData.createInstance(new double[]{
                1.98
        }), true);
        processingGraph.addAttribute(metadataAttribute);

        product.getMetadataRoot().addElement(processingGraph);

        final String metadataXml = L3Mapper.extractProcessingGraphXml(product);
        assertEquals(2304, metadataXml.length());
        assertTrue(metadataXml.contains("1.98"));

    }

    /** pure snap test case based on OLCI AC output L2_of_S3A_OL_1_EFR____20170630T185820_20170630T190020_20171020T121102_0119_019_227______MR1_R_NT_002.SEN3L2_of_.nc
     * to demonstrate failure of mosaicking.
     * Mosaicking in Calvalus uses reprojection of the input to the target grid, then trivial binning.
     * A mask with constant value 1 and fill value 0 is added to the product before reprojection and to read this mask afterwards to identify where there is content.
     * @throws IOException
     * @throws InterruptedException
     * @throws BindingException
     */

    @Ignore
    @Test
    public void testMosaickingSteps() throws IOException, InterruptedException, BindingException {
        System.getProperties().setProperty("snap.pixelGeoCoding.useTiling", "true");
// commenting the following two lines makes the test succeed
        System.getProperties().setProperty("snap.dataio.reader.tileHeight", "512");
        System.getProperties().setProperty("snap.dataio.reader.tileWidth", "512");
        System.getProperties().setProperty("snap.useAlternatePixelGeoCoding", "true");
        //System.getProperties().setProperty("snap.jai.tileCacheSize", "3816");
        final Map<String,String> conf = new HashMap<>();
        conf.put("calvalus.input.pathPatterns", "/home/martin/tmp/c3s/L2_of_S3A_OL_1_EFR____20170630T185820_20170630T190020_20171020T121102_0119_019_227______MR1_R_NT_002.SEN3L2_of_.nc");
        conf.put("calvalus.input.dateRanges", "[2017-06-30:2017-06-30]");
        conf.put("calvalus.minDate", "2017-06-30");
        conf.put("calvalus.maxDate", "2017-06-30");
        conf.put("calvalus.region", "1823,0,58,76");
        conf.put("calvalus.regionGeometry", "POLYGON((-124 39.9,-124 40.2,-123.9 40.2,-123.9 39.9,-124 39.9))");
        conf.put("calvalus.l3.parameters", "<parameters>  <planetaryGrid>org.esa.snap.binning.support.PlateCarreeGrid</planetaryGrid>  <numRows>64800</numRows>  <compositingType>MOSAICKING</compositingType>  <superSampling>1</superSampling>  <maskExpr></maskExpr>  <variables>  <variable>  <name>ndvi</name>  <expr>not nan(sdr_7) ? (sdr_13 - sdr_7) / (sdr_13 + sdr_7) : -1.0</expr>  <validExpr>true</validExpr>  </variable>  </variables>  <aggregators>    <aggregator>      <type>ON_MAX_SET_DEBUG</type>      <onMaxVarName>ndvi</onMaxVarName>    </aggregator>  </aggregators>  </parameters>");
        conf.put("calvalus.output.dir", "/home/martin/tmp/c3s/olci-sr-test-from-nc");

        Product inputProduct = ProductIO.readProduct(new File(conf.get("calvalus.input.pathPatterns")));
        Map<String,Object> subsetParams = new HashMap<>();
        subsetParams.put("region", conf.get("calvalus.region"));
        Product subset = GPF.createProduct("Subset", subsetParams, inputProduct);

        VirtualBand band = new VirtualBand("_binning_mask", ProductData.TYPE_UINT8,
                                           subset.getSceneRasterWidth(), subset.getSceneRasterHeight(),
                                           "true");
        subset.addBand(band);

        BinningConfig binningConfig = BinningConfig.fromXml(conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS));
        DataPeriod dataPeriod = null;
        Geometry regionGeometry = GeometryUtils.parseWKT(conf.get("calvalus.regionGeometry"));
//        if (regionGeometry != null && !GeometryUtils.isGlobalCoverageGeometry(regionGeometry)) {
//            int unwrapDateline = DateLineOps.unwrapDateline(regionGeometry, -180, 180);
//            if (unwrapDateline > 0) {
//                regionGeometry = DateLineOps.pageGeom(regionGeometry, -180, 180);
//            }
//        }
        VariableContext variableContext = binningConfig.createVariableContext();
        Aggregator[] aggregators = binningConfig.createAggregators(variableContext);
        BinManager binManager = new BinManager(variableContext, binningConfig.getPostProcessorConfig(),
                                               aggregators);
        PlanetaryGrid planetaryGrid1 = binningConfig.createPlanetaryGrid();
        Integer superSampling = binningConfig.getSuperSampling();
        BinningContext binningContext = new BinningContextImpl(planetaryGrid1,
                                                               binManager,
                                                               binningConfig.getCompositingType(),
                                                               superSampling != null ? superSampling : 1,
                                                               dataPeriod,
                                                               regionGeometry);
        MosaickingGrid planetaryGrid = (MosaickingGrid) binningContext.getPlanetaryGrid();

        Product reproProduct = planetaryGrid.reprojectToGrid(subset);

        Map<String,Object> subsetParams2 = new HashMap<>();
        subsetParams2.put("geoRegion", conf.get("calvalus.regionGeometry"));
        Product subset2 = GPF.createProduct("Subset", subsetParams2, reproProduct);

        assertEquals(subset2.getBand("_binning_mask").getSampleInt(10,40), 1);

        //ProductIO.writeProduct(subset2, conf.get("calvalus.output.dir") + "/test.dim", "BEAM-DIMAP");
    }

    // for use in debugger sessions
    public static void saveGeoSubset(Product product) throws IOException {
        final Configuration conf = new Configuration();
        conf.set("calvalus.regionGeometry", "POLYGON((-124 39.9,-124 40.2,-123.9 40.2,-123.9 39.9,-124 39.9))");
        conf.set("calvalus.output.dir", "/home/martin/tmp/c3s/olci-sr-test-from-nc");
        Map<String,Object> subsetParams = new HashMap<>();
        subsetParams.put("geoRegion", conf.get("calvalus.regionGeometry"));
        Product subset2 = GPF.createProduct("Subset", subsetParams, product);
        ProductIO.writeProduct(subset2, conf.get("calvalus.output.dir") + "/testgeosubset.dim", "BEAM-DIMAP");
    }

    // for use in debugger sessions
    public static void savePixelSubset(Product product) throws IOException {
        final Configuration conf = new Configuration();
        conf.set("calvalus.region", "20160,17928,36,108");
        conf.set("calvalus.output.dir", "/home/martin/tmp/c3s/olci-sr-test-from-nc");
        Map<String,Object> subsetParams = new HashMap<>();
        subsetParams.put("region", conf.get("calvalus.region"));
        Product subset2 = GPF.createProduct("Subset", subsetParams, product);
        ProductIO.writeProduct(subset2, conf.get("calvalus.output.dir") + "/testpixelsubset.dim", "BEAM-DIMAP");
    }

    @Test
    public void testCrsGridConversion() throws FactoryException, TransformException {
        SystemUtils.initGeoTools();
        MapProjection.class.getClassLoader().setClassAssertionStatus(MapProjection.class.getName(), false);
        CrsGeoCoding inputGeoCoding = new CrsGeoCoding(CRS.decode("EPSG:32636"),
                                                     1830,
                                                     1830,
                                                     166020,
                                                     5100000,
                                                     60,
                                                     -60,
                                                     0,
                                                     0);
        System.out.println(inputGeoCoding);
        double epsDegFor1m = 180.0 / 20000000;
        assertEquals("eps", epsDegFor1m, 0.000009, epsDegFor1m);
        GeoPos geoPos = new GeoPos();
        inputGeoCoding.getGeoPos(new PixelPos(0/60+0.5, 0/60+0.5), geoPos);
        assertEquals("lat", 45.972471023394014, geoPos.lat, epsDegFor1m);
        assertEquals("lon", 28.689203148615423, geoPos.lon, epsDegFor1m);

        CrsGrid grid = new CrsGrid(60.0, "EPSG:32636");
        long binIndex = grid.getBinIndex(geoPos.lat, geoPos.lon);
        assertEquals("bin", 784687239, binIndex);

        double[] centerLatLon = grid.getCenterLatLon(binIndex);
        assertEquals("lat", geoPos.lat, centerLatLon[0], epsDegFor1m);
        assertEquals("lon", geoPos.lon, centerLatLon[1], epsDegFor1m);

        long binIndex2 = grid.getBinIndex(centerLatLon[0], centerLatLon[1]);
        assertEquals("bin", binIndex, binIndex2);

        double[] centerLatLon2 = grid.getCenterLatLon(binIndex2);
        assertEquals("lat", centerLatLon[0], centerLatLon2[0], epsDegFor1m);
        assertEquals("lon", centerLatLon[1], centerLatLon2[1], epsDegFor1m);
    }

    @Test
    public void testWkt() throws FactoryException {
        CoordinateReferenceSystem crs = CRS.decode("EPSG:32636");
        assertEquals("crs", "PROJCS[\"WGS 84 / UTM zone 36N\", \n" +
                "  GEOGCS[\"WGS 84\", \n" +
                "    DATUM[\"World Geodetic System 1984\", \n" +
                "      SPHEROID[\"WGS 84\", 6378137.0, 298.257223563, AUTHORITY[\"EPSG\",\"7030\"]], \n" +
                "      AUTHORITY[\"EPSG\",\"6326\"]], \n" +
                "    PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], \n" +
                "    UNIT[\"degree\", 0.017453292519943295], \n" +
                "    AXIS[\"Geodetic longitude\", EAST], \n" +
                "    AXIS[\"Geodetic latitude\", NORTH], \n" +
                "    AUTHORITY[\"EPSG\",\"4326\"]], \n" +
                "  PROJECTION[\"Transverse_Mercator\", AUTHORITY[\"EPSG\",\"9807\"]], \n" +
                "  PARAMETER[\"central_meridian\", 33.0], \n" +
                "  PARAMETER[\"latitude_of_origin\", 0.0], \n" +
                "  PARAMETER[\"scale_factor\", 0.9996], \n" +
                "  PARAMETER[\"false_easting\", 500000.0], \n" +
                "  PARAMETER[\"false_northing\", 0.0], \n" +
                "  UNIT[\"m\", 1.0], \n" +
                "  AXIS[\"Easting\", EAST], \n" +
                "  AXIS[\"Northing\", NORTH], \n" +
                "  AUTHORITY[\"EPSG\",\"32636\"]]", crs.toWKT());
        Envelope envelope = CRS.getEnvelope(crs);
        assertEquals("min0", 166021.4430960772, envelope.getMinimum(0), 0.0001);
        assertEquals("max0", 833978.5569039235, envelope.getMaximum(0), 0.0001);
        assertEquals("min1", 0.0, envelope.getMinimum(1), 0.0001);
        assertEquals("max1", 9329005.182450451, envelope.getMaximum(1), 0.0001);
        CoordinateReferenceSystem crs2 = CRS.decode("EPSG:4326");
        assertEquals("crs", "GEOGCS[\"WGS 84\", \n" +
                "  DATUM[\"World Geodetic System 1984\", \n" +
                "    SPHEROID[\"WGS 84\", 6378137.0, 298.257223563, AUTHORITY[\"EPSG\",\"7030\"]], \n" +
                "    AUTHORITY[\"EPSG\",\"6326\"]], \n" +
                "  PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], \n" +
                "  UNIT[\"degree\", 0.017453292519943295], \n" +
                "  AXIS[\"Geodetic longitude\", EAST], \n" +
                "  AXIS[\"Geodetic latitude\", NORTH], \n" +
                "  AUTHORITY[\"EPSG\",\"4326\"]]", crs2.toWKT());
        Envelope envelope2 = CRS.getEnvelope(crs2);
        assertEquals("min0", -180.0, envelope2.getMinimum(0), 0.0001);
        assertEquals("max0", 180.0, envelope2.getMaximum(0), 0.0001);
        assertEquals("min1", -90.0, envelope2.getMinimum(1), 0.0001);
        assertEquals("max1", 90.0, envelope2.getMaximum(1), 0.0001);
        final AffineTransform at = new AffineTransform();
        at.translate(0.0, 0.0);
        at.scale(0.1, -0.2);
        at.translate(-30.0, -40.0);
        assertEquals("at", "AffineTransform[[0.1, 0.0, -3.0], [0.0, -0.2, 8.0]]", at.toString());
    }

}
