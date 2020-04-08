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
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

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

}
