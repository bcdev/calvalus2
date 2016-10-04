package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.binning.CompositingType;
import org.esa.snap.binning.aggregators.AggregatorAverage;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.VariableConfig;

import java.time.Year;

class S2Strategy implements SensorStrategy {

    private final PixelProductAreaProvider areaProvider;

    S2Strategy() {
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
    public Workflow getWorkflow(WorkflowConfig workflowConfig) {
        Workflow workflow = new Workflow.Parallel();
        workflow.setSustainable(false);
        Configuration jobConfig = workflowConfig.jobConfig;
        String area = workflowConfig.area;
        String year = workflowConfig.year;
        String month = workflowConfig.month;
        String outputDir = workflowConfig.outputDir;
        String userName = workflowConfig.userName;
        String productionName = workflowConfig.productionName;
        HadoopProcessingService processingService = workflowConfig.processingService;

        PixelProductArea pixelProductArea = new S2Strategy().getArea(area);

        BinningConfig l3Config = getBinningConfig();
        String l3ConfigXml = l3Config.toXml();
        GeometryFactory gf = new GeometryFactory();
        Geometry regionGeometry = new Polygon(new LinearRing(new PackedCoordinateSequence.Float(new double[]{
                pixelProductArea.left - 180, pixelProductArea.top - 90,
                pixelProductArea.right - 180, pixelProductArea.top - 90,
                pixelProductArea.right - 180, pixelProductArea.bottom - 90,
                pixelProductArea.left - 180, pixelProductArea.bottom - 90,
                pixelProductArea.left - 180, pixelProductArea.top - 90
        }, 2), gf), new LinearRing[0], gf);

        String inputPathPattern = String.format("hdfs://calvalus/calvalus/projects/fire/s2-ba/BA-T32NPN-%s%s.*.nc", year, month);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, inputPathPattern);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, area);
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        jobConfig.set(JobConfigNames.CALVALUS_L3_PARAMETERS, l3ConfigXml);
        jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry.toString());

        int day = Year.of(Integer.parseInt(year)).atMonth(Integer.parseInt(month)).atEndOfMonth().getDayOfMonth();
        String minDate = String.format("%s-%s-01", year, month);
        String maxDate = String.format("%s-%s-%s", year, month, day);
        jobConfig.set(JobConfigNames.CALVALUS_MIN_DATE, minDate);
        jobConfig.set(JobConfigNames.CALVALUS_MAX_DATE, maxDate);

        WorkflowItem item = new L3WorkflowItem(processingService, userName, productionName, jobConfig);
        workflow.add(item);

        return workflow;
    }

    private static BinningConfig getBinningConfig() {
        BinningConfig binningConfig = new BinningConfig();
        binningConfig.setCompositingType(CompositingType.MOSAICKING);
        binningConfig.setNumRows(1001878);
        binningConfig.setSuperSampling(1);
        binningConfig.setMaskExpr("result == 999");
        VariableConfig variableConfig = new VariableConfig("result_source", "result");
        binningConfig.setVariableConfigs(variableConfig);
        AggregatorAverage.Config config = new AggregatorAverage.Config("result_source", "result_mosaicked", 0.0, true, true);
        binningConfig.setAggregatorConfigs(config);
        return binningConfig;
    }

    private static class S2PixelProductAreaProvider implements PixelProductAreaProvider {

        private enum S2PixelProductArea {
            AREA_1(154, 90, 159, 95, 1, "area_1"),
            AREA_2(159, 90, 164, 95, 2, "area_2"),
            AREA_3(164, 90, 169, 95, 3, "area_3"),
            AREA_4(169, 90, 174, 95, 4, "area_4"),
            AREA_5(174, 90, 179, 95, 5, "area_5"),
            AREA_6(179, 90, 184, 95, 6, "area_6"),
            AREA_7(184, 90, 189, 95, 7, "area_7"),
            AREA_8(189, 90, 194, 95, 8, "area_8"),
            AREA_9(194, 90, 199, 95, 9, "area_9"),
            AREA_10(199, 90, 204, 95, 10, "area_10"),
            AREA_11(204, 90, 209, 95, 11, "area_11"),
            AREA_12(209, 90, 214, 95, 12, "area_12"),
            AREA_13(214, 90, 219, 95, 13, "area_13"),
            AREA_14(219, 90, 224, 95, 14, "area_14"),
            AREA_15(224, 90, 229, 95, 15, "area_15"),
            AREA_16(229, 90, 233, 95, 16, "area_16"),
            AREA_17(154, 95, 159, 100, 17, "area_17"),
            AREA_18(159, 95, 164, 100, 18, "area_18"),
            AREA_19(164, 95, 169, 100, 19, "area_19"),
            AREA_20(169, 95, 174, 100, 20, "area_20"),
            AREA_21(174, 95, 179, 100, 21, "area_21"),
            AREA_22(179, 95, 184, 100, 22, "area_22"),
            AREA_23(184, 95, 189, 100, 23, "area_23"),
            AREA_24(189, 95, 194, 100, 24, "area_24"),
            AREA_25(194, 95, 199, 100, 25, "area_25"),
            AREA_26(199, 95, 204, 100, 26, "area_26"),
            AREA_27(204, 95, 209, 100, 27, "area_27"),
            AREA_28(209, 95, 214, 100, 28, "area_28"),
            AREA_29(214, 95, 219, 100, 29, "area_29"),
            AREA_30(219, 95, 224, 100, 30, "area_30"),
            AREA_31(224, 95, 229, 100, 31, "area_31"),
            AREA_32(229, 95, 233, 100, 32, "area_32"),
            AREA_33(154, 100, 159, 105, 33, "area_33"),
            AREA_34(159, 100, 164, 105, 34, "area_34"),
            AREA_35(164, 100, 169, 105, 35, "area_35"),
            AREA_36(169, 100, 174, 105, 36, "area_36"),
            AREA_37(174, 100, 179, 105, 37, "area_37"),
            AREA_38(179, 100, 184, 105, 38, "area_38"),
            AREA_39(184, 100, 189, 105, 39, "area_39"),
            AREA_40(189, 100, 194, 105, 40, "area_40"),
            AREA_41(194, 100, 199, 105, 41, "area_41"),
            AREA_42(199, 100, 204, 105, 42, "area_42"),
            AREA_43(204, 100, 209, 105, 43, "area_43"),
            AREA_44(209, 100, 214, 105, 44, "area_44"),
            AREA_45(214, 100, 219, 105, 45, "area_45"),
            AREA_46(219, 100, 224, 105, 46, "area_46"),
            AREA_47(224, 100, 229, 105, 47, "area_47"),
            AREA_48(229, 100, 233, 105, 48, "area_48"),
            AREA_49(154, 105, 159, 110, 49, "area_49"),
            AREA_50(159, 105, 164, 110, 50, "area_50"),
            AREA_51(164, 105, 169, 110, 51, "area_51"),
            AREA_52(169, 105, 174, 110, 52, "area_52"),
            AREA_53(174, 105, 179, 110, 53, "area_53"),
            AREA_54(179, 105, 184, 110, 54, "area_54"),
            AREA_55(184, 105, 189, 110, 55, "area_55"),
            AREA_56(189, 105, 194, 110, 56, "area_56"),
            AREA_57(194, 105, 199, 110, 57, "area_57"),
            AREA_58(199, 105, 204, 110, 58, "area_58"),
            AREA_59(204, 105, 209, 110, 59, "area_59"),
            AREA_60(209, 105, 214, 110, 60, "area_60"),
            AREA_61(214, 105, 219, 110, 61, "area_61"),
            AREA_62(219, 105, 224, 110, 62, "area_62"),
            AREA_63(224, 105, 229, 110, 63, "area_63"),
            AREA_64(229, 105, 233, 110, 64, "area_64"),
            AREA_65(154, 110, 159, 115, 65, "area_65"),
            AREA_66(159, 110, 164, 115, 66, "area_66"),
            AREA_67(164, 110, 169, 115, 67, "area_67"),
            AREA_68(169, 110, 174, 115, 68, "area_68"),
            AREA_69(174, 110, 179, 115, 69, "area_69"),
            AREA_70(179, 110, 184, 115, 70, "area_70"),
            AREA_71(184, 110, 189, 115, 71, "area_71"),
            AREA_72(189, 110, 194, 115, 72, "area_72"),
            AREA_73(194, 110, 199, 115, 73, "area_73"),
            AREA_74(199, 110, 204, 115, 74, "area_74"),
            AREA_75(204, 110, 209, 115, 75, "area_75"),
            AREA_76(209, 110, 214, 115, 76, "area_76"),
            AREA_77(214, 110, 219, 115, 77, "area_77"),
            AREA_78(219, 110, 224, 115, 78, "area_78"),
            AREA_79(224, 110, 229, 115, 79, "area_79"),
            AREA_80(229, 110, 233, 115, 80, "area_80");

            final int left;
            final int top;
            final int right;
            final int bottom;
            final int index;
            final String nicename;

            S2PixelProductArea(int left, int top, int right, int bottom, int index, String nicename) {
                this.left = left;
                this.top = top;
                this.right = right;
                this.bottom = bottom;
                this.index = index;
                this.nicename = nicename;
            }
        }

        @Override
        public PixelProductArea getArea(String identifier) {
            return translate(S2PixelProductArea.valueOf(identifier));
        }

        private static PixelProductArea translate(S2PixelProductArea mppa) {
            return new PixelProductArea(mppa.left, mppa.top, mppa.right, mppa.bottom, mppa.index, mppa.nicename);
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


    }
}
