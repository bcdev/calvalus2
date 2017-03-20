package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.fire.format.grid.s2.S2GridInputFormat;
import com.bc.calvalus.processing.fire.format.grid.s2.S2GridMapper;
import com.bc.calvalus.processing.fire.format.grid.s2.S2GridReducer;
import com.bc.calvalus.processing.fire.format.pixel.s2.JDAggregator;
import com.bc.calvalus.processing.fire.format.pixel.s2.S2FinaliseMapper;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.hadoop.PatternBasedInputFormat;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;
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
        }, 2), gf), new LinearRing[0], gf);

        String tiles = getTiles(pixelProductArea);
        String tilesSpec = "(" + tiles + ")";

        String inputDateSpec = getInputDatePattern(Integer.parseInt(year), Integer.parseInt(month));
        String inputPathPattern = String.format("hdfs://calvalus/calvalus/projects/fire/s2-ba/.*/BA-T%s-%s.*.nc", tilesSpec, inputDateSpec);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, inputPathPattern);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, area);
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
        jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry.toString());

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
        String key = String.format("x%sy%s", pixelProductArea.left, pixelProductArea.top);
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
        AggregatorConfig aggConfig = new JDAggregator.Config("JD", "CL", year, month);
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
            job.setMapperClass(S2FinaliseMapper.class);
            job.setNumReduceTasks(0);
            FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));

        }

        @Override
        protected String[][] getJobConfigDefaults() {
            return new String[0][];
        }
    }

    private static class S2PixelProductAreaProvider implements PixelProductAreaProvider {

        private enum S2PixelProductArea {
            h30v17(150, 90, 155, 95),
            h31v17(155, 90, 160, 95),
            h32v17(160, 90, 165, 95),
            h33v17(165, 90, 170, 95),
            h34v17(170, 90, 175, 95),
            h35v17(175, 90, 180, 95),
            h36v17(180, 90, 185, 95),
            h37v17(185, 90, 190, 95),
            h38v17(190, 90, 195, 95),
            h39v17(195, 90, 200, 95),
            h40v17(200, 90, 205, 95),
            h41v17(205, 90, 210, 95),
            h42v17(210, 90, 215, 95),
            h43v17(215, 90, 220, 95),
            h44v17(220, 90, 225, 95),
            h45v17(225, 90, 230, 95),
            h46v17(230, 90, 235, 95),
            h30v16(150, 95, 155, 100),
            h31v16(155, 95, 160, 100),
            h32v16(160, 95, 165, 100),
            h33v16(165, 95, 170, 100),
            h34v16(170, 95, 175, 100),
            h35v16(175, 95, 180, 100),
            h36v16(180, 95, 185, 100),
            h37v16(185, 95, 190, 100),
            h38v16(190, 95, 195, 100),
            h39v16(195, 95, 200, 100),
            h40v16(200, 95, 205, 100),
            h41v16(205, 95, 210, 100),
            h42v16(210, 95, 215, 100),
            h43v16(215, 95, 220, 100),
            h44v16(220, 95, 225, 100),
            h45v16(225, 95, 230, 100),
            h46v16(230, 95, 235, 100),
            h30v15(150, 100, 155, 105),
            h31v15(155, 100, 160, 105),
            h32v15(160, 100, 165, 105),
            h33v15(165, 100, 170, 105),
            h34v15(170, 100, 175, 105),
            h35v15(175, 100, 180, 105),
            h36v15(180, 100, 185, 105),
            h37v15(185, 100, 190, 105),
            h38v15(190, 100, 195, 105),
            h39v15(195, 100, 200, 105),
            h40v15(200, 100, 205, 105),
            h41v15(205, 100, 210, 105),
            h42v15(210, 100, 215, 105),
            h43v15(215, 100, 220, 105),
            h44v15(220, 100, 225, 105),
            h45v15(225, 100, 230, 105),
            h46v15(230, 100, 235, 105),
            h30v14(150, 105, 155, 110),
            h31v14(155, 105, 160, 110),
            h32v14(160, 105, 165, 110),
            h33v14(165, 105, 170, 110),
            h34v14(170, 105, 175, 110),
            h35v14(175, 105, 180, 110),
            h36v14(180, 105, 185, 110),
            h37v14(185, 105, 190, 110),
            h38v14(190, 105, 195, 110),
            h39v14(195, 105, 200, 110),
            h40v14(200, 105, 205, 110),
            h41v14(205, 105, 210, 110),
            h42v14(210, 105, 215, 110),
            h43v14(215, 105, 220, 110),
            h44v14(220, 105, 225, 110),
            h45v14(225, 105, 230, 110),
            h46v14(230, 105, 235, 110),
            h30v13(150, 110, 155, 115),
            h31v13(155, 110, 160, 115),
            h32v13(160, 110, 165, 115),
            h33v13(165, 110, 170, 115),
            h34v13(170, 110, 175, 115),
            h35v13(175, 110, 180, 115),
            h36v13(180, 110, 185, 115),
            h37v13(185, 110, 190, 115),
            h38v13(190, 110, 195, 115),
            h39v13(195, 110, 200, 115),
            h40v13(200, 110, 205, 115),
            h41v13(205, 110, 210, 115),
            h42v13(210, 110, 215, 115),
            h43v13(215, 110, 220, 115),
            h44v13(220, 110, 225, 115),
            h45v13(225, 110, 230, 115),
            h46v13(230, 110, 235, 115);

            final int left;
            final int top;
            final int right;
            final int bottom;

            S2PixelProductArea(int left, int top, int right, int bottom) {
                this.left = left;
                this.top = top;
                this.right = right;
                this.bottom = bottom;
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
