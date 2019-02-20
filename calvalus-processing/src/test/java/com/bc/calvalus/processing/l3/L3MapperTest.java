package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.beam.SimpleOutputFormat;
import com.bc.calvalus.processing.hadoop.NoRecordReader;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.bc.ceres.glevel.MultiLevelImage;
import com.vividsolutions.jts.geom.Geometry;
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
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.CompositingType;
import org.esa.snap.binning.DataPeriod;
import org.esa.snap.binning.MosaickingGrid;
import org.esa.snap.binning.PlanetaryGrid;
import org.esa.snap.binning.SpatialBinner;
import org.esa.snap.binning.VariableContext;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.SpatialProductBinner;
import org.esa.snap.binning.support.BinTracer;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.StopWatch;
import org.esa.snap.core.util.SystemUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.awt.Dimension;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

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
        assertEquals(2259, metadataXml.length());
        assertTrue(metadataXml.contains("1.98"));

    }

    @Ignore
    @Test
    public void testMapperRunMosaicking() throws IOException, InterruptedException {
        final Configuration conf = new Configuration();
        conf.set("mapreduce.job.inputformat.class", "com.bc.calvalus.processing.hadoop.PatternBasedInputFormat");
        conf.set("mapreduce.job.map.class", "com.bc.calvalus.processing.l3.L3Mapper");
        conf.set("mapreduce.job.partitioner.class", "com.bc.calvalus.processing.l3.L3Partitioner");
        conf.set("mapreduce.job.reduce.class", "com.bc.calvalus.processing.l3.L3Reducer");
        conf.set("mapreduce.job.output.key.class", "org.apache.hadoop.io.LongWritable");
        conf.set("mapreduce.job.output.value.class", "com.bc.calvalus.processing.l3.L3TemporalBin");
        conf.set("mapreduce.job.outputformat.class", "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        conf.set("mapreduce.map.output.key.class", "org.apache.hadoop.io.LongWritable");
        conf.set("mapreduce.map.output.value.class", "com.bc.calvalus.processing.l3.L3SpatialBin");
        conf.set("calvalus.system.snap.pixelGeoCoding.useTiling", "true");
        conf.set("calvalus.system.snap.dataio.reader.tileHeight", "512");
        conf.set("calvalus.system.snap.dataio.reader.tileWidth", "512");
        conf.set("mapreduce.job.name", "olci sr h05v05 2017-06-30 from nc");
        conf.set("calvalus.input.pathPatterns", "/home/martin/tmp/c3s/L2_of_S3A_OL_1_EFR____20170630T185820_20170630T190020_20171020T121102_0119_019_227______MR1_R_NT_002.SEN3L2_of_.nc");
        conf.set("calvalus.input.dateRanges", "[2017-06-30:2017-06-30]");
        conf.set("calvalus.minDate", "2017-06-30");
        conf.set("calvalus.maxDate", "2017-06-30");
        //conf.set("calvalus.regionGeometry", "POLYGON((-124 39.9,-124 39.99,-123.9 39.99,-123.9 39.9,-124 39.9))");
        conf.set("calvalus.regionGeometry", "POLYGON((-124 39.9,-124 40.2,-123.9 40.2,-123.9 39.9,-124 39.9))");
        conf.set("calvalus.l3.parameters", "<parameters>  <planetaryGrid>org.esa.snap.binning.support.PlateCarreeGrid</planetaryGrid>  <numRows>64800</numRows>  <compositingType>MOSAICKING</compositingType>  <superSampling>1</superSampling>  <maskExpr></maskExpr>  <variables>  <variable>  <name>ndvi</name>  <expr>not nan(sdr_7) ? (sdr_13 - sdr_7) / (sdr_13 + sdr_7) : -1.0</expr>  <validExpr>true</validExpr>  </variable>  </variables>  <aggregators>    <aggregator>      <type>ON_MAX_SET_DEBUG</type>      <onMaxVarName>ndvi</onMaxVarName>    </aggregator>  </aggregators>  </parameters>");
        conf.set("calvalus.output.dir", "/calvalus/projects/c3s/olci-sr-test-from-nc");
        conf.set("mapreduce.output.fileoutputformat.outputdir", "/calvalus/projects/c3s/olci-sr-test-from-nc");
        conf.set("calvalus.system.snap.jai.tileCacheSize", "3816");
        conf.set("calvalus.system.snap.useAlternatePixelGeoCoding", "true");

        final FileSplit split = new ProductSplit(new Path(conf.get("calvalus.input.pathPatterns")), 1, new String[] { "localhost" });
        final NoRecordReader reader = new NoRecordReader();
        final StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();

        final String productionName = "test";
        final TaskAttemptID task = new TaskAttemptID(productionName, 0, TaskType.MAP, 0, 0);
        final TaskAttemptContextImpl taskContext = new TaskAttemptContextImpl(conf, task, reporter);
        final OutputCommitter committer = new SimpleOutputFormat().getOutputCommitter(taskContext);

        final MapContextImpl<NullWritable, NullWritable, LongWritable, L3SpatialBin> mapContext =
                new MapContextImpl<>(conf, task, reader, null, committer, reporter, split);
        final Mapper<NullWritable, NullWritable, LongWritable, L3SpatialBin>.Context context =
                new WrappedMapper<NullWritable, NullWritable, LongWritable, L3SpatialBin>().getMapContext(mapContext);

        final L3Mapper mapper = new L3Mapper();
        mapper.run(context);
        committer.commitTask(taskContext);
    }

    @Ignore
    @Test
    public void testMapperRunBinning() throws IOException, InterruptedException {
        final Configuration conf = new Configuration();
        conf.set("mapreduce.job.inputformat.class", "com.bc.calvalus.processing.hadoop.PatternBasedInputFormat");
        conf.set("mapreduce.job.map.class", "com.bc.calvalus.processing.l3.L3Mapper");
        conf.set("mapreduce.job.partitioner.class", "com.bc.calvalus.processing.l3.L3Partitioner");
        conf.set("mapreduce.job.reduce.class", "com.bc.calvalus.processing.l3.L3Reducer");
        conf.set("mapreduce.job.output.key.class", "org.apache.hadoop.io.LongWritable");
        conf.set("mapreduce.job.output.value.class", "com.bc.calvalus.processing.l3.L3TemporalBin");
        conf.set("mapreduce.job.outputformat.class", "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat");
        conf.set("mapreduce.map.output.key.class", "org.apache.hadoop.io.LongWritable");
        conf.set("mapreduce.map.output.value.class", "com.bc.calvalus.processing.l3.L3SpatialBin");
        conf.set("calvalus.system.snap.pixelGeoCoding.useTiling", "true");
        conf.set("calvalus.system.snap.dataio.reader.tileHeight", "512");
        conf.set("calvalus.system.snap.dataio.reader.tileWidth", "512");
        conf.set("mapreduce.job.name", "olci sr h05v05 2017-06-30 from nc");
        conf.set("calvalus.input.pathPatterns", "/home/martin/tmp/c3s/L2_of_S3A_OL_1_EFR____20170630T185820_20170630T190020_20171020T121102_0119_019_227______MR1_R_NT_002.SEN3L2_of_.nc");
        conf.set("calvalus.input.dateRanges", "[2017-06-30:2017-06-30]");
        conf.set("calvalus.minDate", "2017-06-30");
        conf.set("calvalus.maxDate", "2017-06-30");
        //conf.set("calvalus.regionGeometry", "POLYGON((-124 39.9,-124 39.99,-123.9 39.99,-123.9 39.9,-124 39.9))");
        conf.set("calvalus.regionGeometry", "POLYGON((-124 39.9,-124 40.2,-123.9 40.2,-123.9 39.9,-124 39.9))");
        conf.set("calvalus.l3.parameters", "<parameters>  <planetaryGrid>org.esa.snap.binning.support.PlateCarreeGrid</planetaryGrid>  <numRows>64800</numRows>  <compositingType>BINNING</compositingType>  <superSampling>1</superSampling>  <maskExpr></maskExpr>  <variables>  <variable>  <name>ndvi</name>  <expr>not nan(sdr_7) ? (sdr_13 - sdr_7) / (sdr_13 + sdr_7) : -1.0</expr>  <validExpr>true</validExpr>  </variable>  </variables>  <aggregators>    <aggregator>      <type>ON_MAX_SET_DEBUG</type>      <onMaxVarName>ndvi</onMaxVarName>    </aggregator>  </aggregators>  </parameters>");
        conf.set("calvalus.output.dir", "/calvalus/projects/c3s/olci-sr-test-from-nc");
        conf.set("mapreduce.output.fileoutputformat.outputdir", "/calvalus/projects/c3s/olci-sr-test-from-nc");
        conf.set("calvalus.system.snap.jai.tileCacheSize", "3816");
        conf.set("calvalus.system.snap.useAlternatePixelGeoCoding", "true");

        final FileSplit split = new ProductSplit(new Path(conf.get("calvalus.input.pathPatterns")), 1, new String[] { "localhost" });
        final NoRecordReader reader = new NoRecordReader();
        final StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();

        final String productionName = "test";
        final TaskAttemptID task = new TaskAttemptID(productionName, 0, TaskType.MAP, 0, 0);
        final TaskAttemptContextImpl taskContext = new TaskAttemptContextImpl(conf, task, reporter);
        final OutputCommitter committer = new SimpleOutputFormat().getOutputCommitter(taskContext);

        final MapContextImpl<NullWritable, NullWritable, LongWritable, L3SpatialBin> mapContext =
                new MapContextImpl<>(conf, task, reader, null, committer, reporter, split);
        final Mapper<NullWritable, NullWritable, LongWritable, L3SpatialBin>.Context context =
                new WrappedMapper<NullWritable, NullWritable, LongWritable, L3SpatialBin>().getMapContext(mapContext);

        final L3Mapper mapper = new L3Mapper();
        mapper.run(context);
        committer.commitTask(taskContext);
    }

    public static void saveGeoSubset(Product product) throws IOException {
        final Configuration conf = new Configuration();
        conf.set("calvalus.regionGeometry", "POLYGON((-124 39.9,-124 40.2,-123.9 40.2,-123.9 39.9,-124 39.9))");
        conf.set("calvalus.output.dir", "/home/martin/tmp/c3s/olci-sr-test-from-nc");
        Map<String,Object> subsetParams = new HashMap<>();
        subsetParams.put("geoRegion", conf.get("calvalus.regionGeometry"));
        Product subset2 = GPF.createProduct("Subset", subsetParams, product);
        ProductIO.writeProduct(subset2, conf.get("calvalus.output.dir") + "/testgeosubset.dim", "BEAM-DIMAP");
    }

    public static void savePixelSubset(Product product) throws IOException {
        final Configuration conf = new Configuration();
        conf.set("calvalus.region", "20160,17928,36,108");
        conf.set("calvalus.output.dir", "/home/martin/tmp/c3s/olci-sr-test-from-nc");
        Map<String,Object> subsetParams = new HashMap<>();
        subsetParams.put("region", conf.get("calvalus.region"));
        Product subset2 = GPF.createProduct("Subset", subsetParams, product);
        ProductIO.writeProduct(subset2, conf.get("calvalus.output.dir") + "/testpixelsubset.dim", "BEAM-DIMAP");
    }

    public static void saveBandSubset(Product product) throws IOException {
        final Configuration conf = new Configuration();
        conf.set("calvalus.bandNames", "_binning_mask");
        conf.set("calvalus.output.dir", "/home/martin/tmp/c3s/olci-sr-test-from-nc");
        Map<String,Object> subsetParams = new HashMap<>();
        subsetParams.put("bandNames", conf.get("calvalus.bandNames"));
        Product subset2 = GPF.createProduct("Subset", subsetParams, product);
        ProductIO.writeProduct(subset2, conf.get("calvalus.output.dir") + "/testbandsubset.dim", "BEAM-DIMAP");
    }

    @Ignore
    @Test
    public void testMosaickingSteps() throws IOException, InterruptedException {
        System.getProperties().setProperty("snap.pixelGeoCoding.useTiling", "true");
//        System.getProperties().setProperty("snap.dataio.reader.tileHeight", "512");
//        System.getProperties().setProperty("snap.dataio.reader.tileWidth", "512");
        System.getProperties().setProperty("snap.useAlternatePixelGeoCoding", "true");
        //System.getProperties().setProperty("snap.jai.tileCacheSize", "3816");
        final Configuration conf = new Configuration();
        conf.set("calvalus.input.pathPatterns", "/home/martin/tmp/c3s/L2_of_S3A_OL_1_EFR____20170630T185820_20170630T190020_20171020T121102_0119_019_227______MR1_R_NT_002.SEN3L2_of_.nc");
        conf.set("calvalus.input.dateRanges", "[2017-06-30:2017-06-30]");
        conf.set("calvalus.minDate", "2017-06-30");
        conf.set("calvalus.maxDate", "2017-06-30");
        conf.set("calvalus.region", "1823,0,58,76");
        conf.set("calvalus.regionGeometry", "POLYGON((-124 39.9,-124 40.2,-123.9 40.2,-123.9 39.9,-124 39.9))");
        conf.set("calvalus.l3.parameters", "<parameters>  <planetaryGrid>org.esa.snap.binning.support.PlateCarreeGrid</planetaryGrid>  <numRows>64800</numRows>  <compositingType>MOSAICKING</compositingType>  <superSampling>1</superSampling>  <maskExpr></maskExpr>  <variables>  <variable>  <name>ndvi</name>  <expr>not nan(sdr_7) ? (sdr_13 - sdr_7) / (sdr_13 + sdr_7) : -1.0</expr>  <validExpr>true</validExpr>  </variable>  </variables>  <aggregators>    <aggregator>      <type>ON_MAX_SET_DEBUG</type>      <onMaxVarName>ndvi</onMaxVarName>    </aggregator>  </aggregators>  </parameters>");
        conf.set("calvalus.output.dir", "/home/martin/tmp/c3s/olci-sr-test-from-nc");

        Product inputProduct = ProductIO.readProduct(new File(conf.get("calvalus.input.pathPatterns")));
        Map<String,Object> subsetParams = new HashMap<>();
        subsetParams.put("region", conf.get("calvalus.region"));
        Product subset = GPF.createProduct("Subset", subsetParams, inputProduct);

        VirtualBand band = new VirtualBand("_binning_mask", ProductData.TYPE_UINT8,
                                           subset.getSceneRasterWidth(), subset.getSceneRasterHeight(),
                                           "true");
        subset.addBand(band);

        BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);
        DataPeriod dataPeriod = HadoopBinManager.createDataPeriod(conf, binningConfig.getMinDataHour());
        Geometry regionGeometry = GeometryUtils.createGeometry(conf.get("calvalus.regionGeometry"));
        BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, dataPeriod, regionGeometry);
        MosaickingGrid planetaryGrid = (MosaickingGrid) binningContext.getPlanetaryGrid();
        Product reproProduct = planetaryGrid.reprojectToGrid(subset);

        Map<String,Object> subsetParams2 = new HashMap<>();
        subsetParams2.put("geoRegion", conf.get("calvalus.regionGeometry"));
        Product subset2 = GPF.createProduct("Subset", subsetParams2, reproProduct);

        ProductIO.writeProduct(subset2, conf.get("calvalus.output.dir") + "/test.dim", "BEAM-DIMAP");
    }


}
