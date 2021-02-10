package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.fire.format.grid.modis.ModisGridInputFormat;
import com.bc.calvalus.processing.fire.format.grid.modis.ModisGridMapper;
import com.bc.calvalus.processing.fire.format.grid.modis.ModisGridReducer;
import com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper;
import com.bc.calvalus.processing.fire.format.pixel.modis.ModisJDAggregator;
import com.bc.calvalus.processing.fire.format.pixel.modis.ModisPixelFinaliseMapper;
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

public class ModisStrategy implements SensorStrategy {

    private final MerisStrategy delegate;

    public ModisStrategy() {
        delegate = new MerisStrategy();
    }

    @Override
    public PixelProductArea getArea(String identifier) {
        return delegate.getArea(identifier);
    }

    @Override
    public PixelProductArea[] getAllAreas() {
        return delegate.getAllAreas();
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
                pixelProductArea.left - 180, 90 - pixelProductArea.top,
                pixelProductArea.right - 180, 90 - pixelProductArea.top,
                pixelProductArea.right - 180, 90 - pixelProductArea.bottom,
                pixelProductArea.left - 180, 90 - pixelProductArea.bottom,
                pixelProductArea.left - 180, 90 - pixelProductArea.top
        }, 2, 0), gf), new LinearRing[0], gf);

        String tiles = getTiles(pixelProductArea);
        String tilesSpec = "(" + tiles + ")";

        String inputPathPattern = String.format("hdfs://calvalus/calvalus/projects/fire/modis-ba/%s/%s/burned_%s_%s_%s.*nc", year, month, year, Integer.parseInt(month), tilesSpec);
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
        workflow.add(new PixelFinaliseWorkflowItem(processingService, userName, productionName, finaliseConfig, area));
        return workflow;
    }

    @Override
    public Class<? extends InputFormat> getGridInputFormat() {
        return ModisGridInputFormat.class;
    }

    @Override
    public Class<? extends Mapper> getGridMapperClass() {
        return ModisGridMapper.class;
    }

    @Override
    public Class<? extends Reducer> getGridReducerClass() {
        return ModisGridReducer.class;
    }

    private static class PixelFinaliseWorkflowItem extends HadoopWorkflowItem {

        private final String area;

        public PixelFinaliseWorkflowItem(HadoopProcessingService processingService, String userName, String productionName, Configuration jobConfig, String area) {
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
            job.setMapperClass(ModisPixelFinaliseMapper.class);
            int year = Integer.parseInt(getJobConfig().get("calvalus.year"));
            int lcYear;
            if (year <= 2016) {
                if (year == 2000) {
                    lcYear = 2000;
                } else {
                    lcYear = year - 1;
                }
            } else {
                lcYear = 2015;
            }
            String lcPath = "hdfs://calvalus/calvalus/projects/fire/aux/modis-lc/ESACCI-LC-L4-LCCS-Map-300m-P1Y-" + lcYear + "-v2.0.7_MODIS.tif";
            job.getConfiguration().set(PixelFinaliseMapper.KEY_LC_PATH, lcPath);
            job.getConfiguration().set(PixelFinaliseMapper.KEY_VERSION, "fv5.1");
            PixelProductArea area = new ModisStrategy().getArea(job.getConfiguration().get("calvalus.area"));
            String areaString = String.format("%s;%s;%s;%s;%s;%s", area.index, area.nicename, area.left, area.top, area.right, area.bottom);
            job.getConfiguration().set(PixelFinaliseMapper.KEY_AREA_STRING, areaString);
            job.setNumReduceTasks(0);
            FileOutputFormat.setOutputPath(job, new Path(getOutputDir()));

        }

        @Override
        protected String[][] getJobConfigDefaults() {
            return new String[0][];
        }
    }

    private static BinningConfig getBinningConfig(int year, int month) {
        BinningConfig binningConfig = new BinningConfig();
        binningConfig.setCompositingType(CompositingType.BINNING);
        binningConfig.setNumRows(80152);
        binningConfig.setSuperSampling(1);
        binningConfig.setMaskExpr("true");
        binningConfig.setPlanetaryGrid("org.esa.snap.binning.support.PlateCarreeGrid");
        AggregatorConfig aggConfig = new ModisJDAggregator.Config("classification", "uncertainty", year, month);

        binningConfig.setAggregatorConfigs(aggConfig);
        return binningConfig;
    }

    private String getTiles(PixelProductArea pixelProductArea) {
        Properties tiles = new Properties();
        try {
            tiles.load(getClass().getResourceAsStream("areas-tiles-modis.properties"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        String key = pixelProductArea.nicename.replace(" ", "_");
        return tiles.getProperty(key);
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
}
