package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.fire.format.grid.s2.S2GridInputFormat;
import com.bc.calvalus.processing.fire.format.grid.s2.S2GridMapper;
import com.bc.calvalus.processing.fire.format.grid.s2.S2GridReducer;
import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import com.bc.calvalus.processing.fire.format.pixel.s2.S2JDAggregator;
import com.bc.calvalus.processing.fire.format.pixel.s2.S2PixelFinaliseMapper;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.snap.binning.AggregatorConfig;
import org.esa.snap.binning.CompositingType;
import org.esa.snap.binning.operator.BinningConfig;

import java.io.IOException;
import java.time.Year;
import java.util.Properties;

public class S2Strategy implements SensorStrategy {

    private final PixelProductAreaProvider areaProvider;

    public S2Strategy() {
        areaProvider = new S2PixelProductAreaProvider();
    }

    @Override
    public PixelProductArea getArea(String identifier) {
        return areaProvider.getArea(identifier);
    }

    @Override
    public PixelProductArea[] getAllAreas() {
        return areaProvider.getAllAreas();
    }

    @Override
    public Workflow getPixelFormattingWorkflow(WorkflowConfig workflowConfig) {
        Workflow workflow = new Workflow.Sequential();
        workflow.setSustainable(false);
        Configuration jobConfig = workflowConfig.jobConfig;
        String area = workflowConfig.area;
        String year = workflowConfig.year;
        String month = workflowConfig.month;
        String outputDir = workflowConfig.outputDir;
        String userName = workflowConfig.userName;
        String productionName = workflowConfig.productionName;
        HadoopProcessingService processingService = workflowConfig.processingService;

        PixelProductArea pixelProductArea = getArea(area);

        BinningConfig l3Config = getBinningConfig(Integer.parseInt(year), Integer.parseInt(month));
        String l3ConfigXml = l3Config.toXml();
        GeometryFactory gf = new GeometryFactory();
        Geometry regionGeometry = new Polygon(new LinearRing(new PackedCoordinateSequence.Float(new double[]{
                pixelProductArea.left - 180, pixelProductArea.top - 90,
                pixelProductArea.right - 180, pixelProductArea.top - 90,
                pixelProductArea.right - 180, pixelProductArea.bottom - 90,
                pixelProductArea.left - 180, pixelProductArea.bottom - 90,
                pixelProductArea.left - 180, pixelProductArea.top - 90
        }, 2, 0), gf), new LinearRing[0], gf);

        String tiles = getTiles(pixelProductArea);
        String tilesSpec = "(" + tiles + ")";

        String inputDateSpec = getInputDatePattern(Integer.parseInt(year), Integer.parseInt(month));
        String inputPathPattern = String.format("hdfs://calvalus/calvalus/projects/fire/s2-ba/.*/BA-T%s-%s.*.nc", tilesSpec, inputDateSpec);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, inputPathPattern);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, area);
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
        jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry.toString());
        jobConfig.set("calvalus.sensor", "MSI");

        int lastDayOfMonth = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atEndOfMonth().getDayOfMonth();
        String minDate = String.format("%s-%s-01", year, month);
        String maxDate = String.format("%s-%s-%02d", year, month, lastDayOfMonth);
        jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, minDate);
        jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, maxDate);

        if (!exists(jobConfig, outputDir, String.format("L3_%s-%s-01_%s-%s-%02d.nc", year, month, year, month, lastDayOfMonth))) {
            WorkflowItem item = new L3WorkflowItem(processingService, userName, productionName, jobConfig);
            workflow.add(item);

            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir + "_format");
            jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, outputDir + "_format");
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, "NetCDF4-CF");

            WorkflowItem formatItem = new L3FormatWorkflowItem(processingService,
                    userName,
                    productionName + " formatting", jobConfig);
            workflow.add(formatItem);
        } else {
            CalvalusLogger.getLogger().info("Skipping binning and formatting, moving on to finalise");
        }

        Configuration finaliseConfig = new Configuration(jobConfig);
        finaliseConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, String.format("%s_format/L3_%s-%s.*.nc", outputDir, year, month));
        workflow.add(new FinaliseWorkflowItem(processingService, userName, productionName, finaliseConfig, area));
        return workflow;
    }

    static String getInputDatePattern(int year, int month) {
        return String.format("%s%02d", year, month);
    }

    private String getTiles(PixelProductArea pixelProductArea) {
        Properties tiles = new Properties();
        try {
            tiles.load(getClass().getResourceAsStream("areas-tiles-5deg.properties"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        String key = String.format("x%sy%s", pixelProductArea.left, pixelProductArea.bottom);
        return tiles.getProperty(key);
    }

    @Override
    public Class<? extends InputFormat> getGridInputFormat() {
        return S2GridInputFormat.class;
    }

    @Override
    public Class<? extends Mapper> getGridMapperClass() {
        return S2GridMapper.class;
    }

    @Override
    public Class<? extends Reducer> getGridReducerClass() {
        return S2GridReducer.class;
    }

    private static boolean exists(Configuration jobConfig, String outputDir, String filename) {
        boolean exists;
        try {
            FileSystem fs = FileSystem.get(jobConfig);
            exists = fs.exists(new Path(outputDir + "_format", filename));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return exists;
    }

    private static BinningConfig getBinningConfig(int year, int month) {
        BinningConfig binningConfig = new BinningConfig();
        binningConfig.setCompositingType(CompositingType.BINNING);
        binningConfig.setNumRows(1001878);
        binningConfig.setSuperSampling(1);
        binningConfig.setMaskExpr("true");
        binningConfig.setPlanetaryGrid("org.esa.snap.binning.support.PlateCarreeGrid");
        AggregatorConfig aggConfig = new S2JDAggregator.Config("JD", "CL", year, month);
        binningConfig.setAggregatorConfigs(aggConfig);
        return binningConfig;
    }

    private static class FinaliseWorkflowItem extends HadoopWorkflowItem {

        private final String area;

        public FinaliseWorkflowItem(HadoopProcessingService processingService, String userName, String productionName, Configuration jobConfig, String area) {
            super(processingService, userName, productionName + " finalisation", jobConfig);
            this.area = area;
        }

        @Override
        public String getOutputDir() {
            String year = getJobConfig().get("calvalus.year");
            String month = getJobConfig().get("calvalus.month");
            return getJobConfig().get(JobConfigNames.CALVALUS_OUTPUT_DIR) + "/" + year + "/" + month + "/" + area + "/" + "final";
        }

        @Override
        protected void configureJob(Job job) throws IOException {
            job.setInputFormatClass(PatternBasedInputFormat.class);
            job.getConfiguration().set(PixelFinaliseMapper.KEY_LC_PATH, "hdfs://calvalus/calvalus/projects/fire/aux/s2-lc/2016.nc");
            job.getConfiguration().set(PixelFinaliseMapper.KEY_VERSION, "fv1.1");
            PixelProductArea area = new S2Strategy().getArea(job.getConfiguration().get("calvalus.area"));
            String areaString = String.format("%s;%s;%s;%s;%s;%s", area.index, area.nicename, area.left, area.top, area.right, area.bottom);
            job.getConfiguration().set(PixelFinaliseMapper.KEY_AREA_STRING, areaString);
            job.setMapperClass(S2PixelFinaliseMapper.class);
            job.setNumReduceTasks(0);
            FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));
        }

        @Override
        protected String[][] getJobConfigDefaults() {
            return new String[0][];
        }
    }

    public static class S2PixelProductAreaProvider implements PixelProductAreaProvider {

        public enum S2PixelProductArea {
            h31v14,
            h31v15,
            h32v13,
            h32v14,
            h32v15,
            h33v13,
            h33v14,
            h33v15,
            h33v16,
            h34v13,
            h34v14,
            h34v15,
            h34v16,
            h34v17,
            h35v13,
            h35v14,
            h35v15,
            h35v16,
            h35v17,
            h36v13,
            h36v14,
            h36v15,
            h36v16,
            h37v13,
            h37v14,
            h37v15,
            h37v16,
            h37v17,
            h37v18,
            h38v13,
            h38v14,
            h38v15,
            h38v16,
            h38v17,
            h38v18,
            h38v19,
            h38v20,
            h38v21,
            h38v22,
            h38v23,
            h39v13,
            h39v14,
            h39v15,
            h39v16,
            h39v17,
            h39v18,
            h39v19,
            h39v20,
            h39v21,
            h39v22,
            h39v23,
            h39v24,
            h40v13,
            h40v14,
            h40v15,
            h40v16,
            h40v17,
            h40v18,
            h40v19,
            h40v20,
            h40v21,
            h40v22,
            h40v23,
            h40v24,
            h41v13,
            h41v14,
            h41v15,
            h41v16,
            h41v17,
            h41v18,
            h41v19,
            h41v20,
            h41v21,
            h41v22,
            h41v23,
            h41v24,
            h42v13,
            h42v14,
            h42v15,
            h42v16,
            h42v17,
            h42v18,
            h42v19,
            h42v20,
            h42v21,
            h42v22,
            h42v23,
            h42v24,
            h43v13,
            h43v14,
            h43v15,
            h43v16,
            h43v17,
            h43v18,
            h43v19,
            h43v20,
            h43v21,
            h43v22,
            h44v14,
            h44v15,
            h44v16,
            h44v17,
            h44v18,
            h44v20,
            h44v21,
            h44v22,
            h44v23,
            h45v15,
            h45v16,
            h45v17,
            h45v20,
            h45v21,
            h45v22,
            h45v23,
            h46v15,
            h46v16,
            h46v20,
            h46v21;

            final int left;
            final int top;
            final int right;
            final int bottom;

            S2PixelProductArea() {
                int h = Integer.parseInt(name().substring(1, 3));
                int v = Integer.parseInt(name().substring(4, 6));
                this.left = h * 5;
                this.top = 180 - (v * 5); // v16 --> 100 --> 10
                this.right = (h +1 )* 5;
                this.bottom = 180 - ((v + 1) * 5); // v16 --> 95 --> 5
            }

        }

        @Override
        public PixelProductArea getArea(String identifier) {
            return translate(S2PixelProductArea.valueOf(identifier));
        }

        @Override
        public PixelProductArea[] getAllAreas() {
            PixelProductArea[] result = new PixelProductArea[S2PixelProductArea.values().length];
            S2PixelProductArea[] values = S2PixelProductArea.values();
            for (int i = 0; i < values.length; i++) {
                S2PixelProductArea area = values[i];
                result[i] = translate(area);
            }
            return result;
        }

        private static PixelProductArea translate(S2PixelProductArea mppa) {
            return new PixelProductArea(mppa.left, mppa.top, mppa.right, mppa.bottom, mppa.name());
        }

    }
}
