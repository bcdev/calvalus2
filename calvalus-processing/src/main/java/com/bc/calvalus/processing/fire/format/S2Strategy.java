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
import org.esa.snap.binning.operator.VariableConfig;

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
                    productionName + " Format", jobConfig);
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
        binningConfig.setCompositingType(CompositingType.MOSAICKING);
        binningConfig.setNumRows(1001878);
        binningConfig.setSuperSampling(1);
        binningConfig.setMaskExpr("true");
        VariableConfig doyConfig = new VariableConfig("day_of_year", "JD");
        VariableConfig clConfig = new VariableConfig("confidence_level", "CL");
        binningConfig.setVariableConfigs(doyConfig, clConfig);
        binningConfig.setPlanetaryGrid("org.esa.snap.binning.support.PlateCarreeGrid");
        AggregatorConfig aggConfig = new JDAggregator.Config("day_of_year", "confidence_level", year, month);
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
            AREA_1(154, 90, 159, 95),
            AREA_2(159, 90, 164, 95),
            AREA_3(164, 90, 169, 95),
            AREA_4(169, 90, 174, 95),
            AREA_5(174, 90, 179, 95),
            AREA_6(179, 90, 184, 95),
            AREA_7(184, 90, 189, 95),
            AREA_8(189, 90, 194, 95),
            AREA_9(194, 90, 199, 95),
            AREA_10(199, 90, 204, 95),
            AREA_11(204, 90, 209, 95),
            AREA_12(209, 90, 214, 95),
            AREA_13(214, 90, 219, 95),
            AREA_14(219, 90, 224, 95),
            AREA_15(224, 90, 229, 95),
            AREA_16(229, 90, 233, 95),
            AREA_17(154, 95, 159, 100),
            AREA_18(159, 95, 164, 100),
            AREA_19(164, 95, 169, 100),
            AREA_20(169, 95, 174, 100),
            AREA_21(174, 95, 179, 100),
            AREA_22(179, 95, 184, 100),
            AREA_23(184, 95, 189, 100),
            AREA_24(189, 95, 194, 100),
            AREA_25(194, 95, 199, 100),
            AREA_26(199, 95, 204, 100),
            AREA_27(204, 95, 209, 100),
            AREA_28(209, 95, 214, 100),
            AREA_29(214, 95, 219, 100),
            AREA_30(219, 95, 224, 100),
            AREA_31(224, 95, 229, 100),
            AREA_32(229, 95, 233, 100),
            AREA_33(154, 100, 159, 105),
            AREA_34(159, 100, 164, 105),
            AREA_35(164, 100, 169, 105),
            AREA_36(169, 100, 174, 105),
            AREA_37(174, 100, 179, 105),
            AREA_38(179, 100, 184, 105),
            AREA_39(184, 100, 189, 105),
            AREA_40(189, 100, 194, 105),
            AREA_41(194, 100, 199, 105),
            AREA_42(199, 100, 204, 105),
            AREA_43(204, 100, 209, 105),
            AREA_44(209, 100, 214, 105),
            AREA_45(214, 100, 219, 105),
            AREA_46(219, 100, 224, 105),
            AREA_47(224, 100, 229, 105),
            AREA_48(229, 100, 233, 105),
            AREA_49(154, 105, 159, 110),
            AREA_50(159, 105, 164, 110),
            AREA_51(164, 105, 169, 110),
            AREA_52(169, 105, 174, 110),
            AREA_53(174, 105, 179, 110),
            AREA_54(179, 105, 184, 110),
            AREA_55(184, 105, 189, 110),
            AREA_56(189, 105, 194, 110),
            AREA_57(194, 105, 199, 110),
            AREA_58(199, 105, 204, 110),
            AREA_59(204, 105, 209, 110),
            AREA_60(209, 105, 214, 110),
            AREA_61(214, 105, 219, 110),
            AREA_62(219, 105, 224, 110),
            AREA_63(224, 105, 229, 110),
            AREA_64(229, 105, 233, 110),
            AREA_65(154, 110, 159, 115),
            AREA_66(159, 110, 164, 115),
            AREA_67(164, 110, 169, 115),
            AREA_68(169, 110, 174, 115),
            AREA_69(174, 110, 179, 115),
            AREA_70(179, 110, 184, 115),
            AREA_71(184, 110, 189, 115),
            AREA_72(189, 110, 194, 115),
            AREA_73(194, 110, 199, 115),
            AREA_74(199, 110, 204, 115),
            AREA_75(204, 110, 209, 115),
            AREA_76(209, 110, 214, 115),
            AREA_77(214, 110, 219, 115),
            AREA_78(219, 110, 224, 115),
            AREA_79(224, 110, 229, 115),
            AREA_80(229, 110, 233, 115);

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
