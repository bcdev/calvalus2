package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.bc.calvalus.processing.ta.TAConfig;
import com.bc.calvalus.processing.ta.TAWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import static com.bc.calvalus.production.hadoop.L3ProductionType.*;

/**
 * Trend analysis: A production type used for generating a time-series generated from L3 products and a numb er of
 * given regions.
 *
 * @author MarcoZ
 * @author Norman
 */
public class TAProductionType extends HadoopProductionType {
    public static final String NAME = "TA";
    private static final String TA_REGIONS_PROPERTIES = "ta-regions.properties";
    static final long MILLIS_PER_DAY = 24L * 60L * 60L * 1000L;

    /*
     public static void main(String[] args) throws IOException {
         File[] files = new File("C:\\Users\\Norman\\Downloads\\cc_sites").listFiles();
         for (File file : files) {
             Properties properties = new Properties();
             FileReader reader = new FileReader(file);
             try {
                 properties.load(reader);
                 String geometry = properties.getProperty("geometry[0]");
                 String s1 = file.getName() + " = " + geometry;
                 System.out.println(s1);
             } finally {
                 reader.close();
             }
         }
     }
    */

    public TAProductionType(HadoopProcessingService processingService, StagingService stagingService) throws ProductionException {
        super(NAME, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = createTAProductionName(productionRequest);

        String inputProductSetId = productionRequest.getParameter("inputProductSetId");
        Date minDate = productionRequest.getDate("minDate");
        Date maxDate = productionRequest.getDate("maxDate");

        String processorName = productionRequest.getParameter("processorName");
        String processorParameters = productionRequest.getParameter("processorParameters");
        String processorBundle = String.format("%s-%s",
                                               productionRequest.getParameter("processorBundleName"),
                                               productionRequest.getParameter("processorBundleVersion"));

        Geometry roiGeometry = productionRequest.getRegionGeometry();

        L3Config l3Config = createL3Config(productionRequest);
        TAConfig taConfig = createTAConfig(productionRequest);

        int periodLength = productionRequest.getInteger("periodLength", 32); // unit=days
        int compositingPeriodLength = productionRequest.getInteger("compositingPeriodLength", periodLength); // unit=days

        final long periodLengthMillis = periodLength * MILLIS_PER_DAY;
        final long compositingPeriodLengthMillis = compositingPeriodLength * MILLIS_PER_DAY;

        long time = minDate.getTime();
        Workflow.Parallel parallel = new Workflow.Parallel();
        for (int i = 0; ; i++) {

            // we subtract 1 ms for date2, because when formatting to date format we would get the following day.
            Date date1 = new Date(time);
            Date date2 = new Date(time + compositingPeriodLengthMillis - 1L);

            String date1Str = ProductionRequest.getDateFormat().format(date1);
            String date2Str = ProductionRequest.getDateFormat().format(date2);

            if (date2.after(new Date(maxDate.getTime() + MILLIS_PER_DAY))) {
                break;
            }

            Workflow.Sequential sequential = new Workflow.Sequential();

            String l3JobName = String.format("%s_%d_L3", productionId, (i + 1));
            String taJobName = String.format("%s_%d_TA", productionId, (i + 1));

            // todo - use geoRegion to filter input files (nf,20.04.2011)
            String[] l1InputFiles = getInputFiles(inputProductSetId, date1, date2);
            String l3OutputDir = getOutputDir(productionRequest.getUserName(), l3JobName);
            String taOutputDir = getOutputDir(productionRequest.getUserName(), taJobName);

            L3WorkflowItem l3WorkflowItem = new L3WorkflowItem(getProcessingService(),
                                                               l3JobName,
                                                               processorBundle,
                                                               processorName,
                                                               processorParameters,
                                                               roiGeometry,
                                                               l1InputFiles,
                                                               l3OutputDir,
                                                               l3Config,
                                                               date1Str,
                                                               date2Str);

            TAWorkflowItem taWorkflowItem = new TAWorkflowItem(getProcessingService(),
                                                               taJobName,
                                                               l3OutputDir,
                                                               taOutputDir,
                                                               l3Config,
                                                               taConfig,
                                                               date1Str,
                                                               date2Str);

            sequential.add(l3WorkflowItem);
            sequential.add(taWorkflowItem);

            parallel.add(sequential);
            time += periodLengthMillis;
        }

        String stagingDir = String.format("%s/%s", productionRequest.getUserName(), productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              parallel);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        return new TAStaging(production,
                             getProcessingService().getJobClient().getConf(),
                             getStagingService().getStagingDir());
    }

    String getOutputDir(String userName, String dirName) {
        return String.format("%s/%s/%s",
                             getProcessingService().getDataOutputPath(),
                             userName,
                             dirName);
    }

    static String createTAProductionName(ProductionRequest productionRequest) throws ProductionException {
        return String.format("Trend analysis using product set '%s' and L2 processor '%s'",
                             productionRequest.getParameter("inputProductSetId"),
                             productionRequest.getParameter("processorName"));

    }

    static TAConfig createTAConfig(ProductionRequest productionRequest) {
        Properties regions = loadRegions(TA_REGIONS_PROPERTIES);
        ArrayList<TAConfig.RegionConfiguration> regionList = createRegionList(regions);
        return new TAConfig(regionList.toArray(new TAConfig.RegionConfiguration[regionList.size()]));
    }

    private static ArrayList<TAConfig.RegionConfiguration> createRegionList(Properties properties) {
        WKTReader wktReader = new WKTReader();
        Set<String> regionNames = properties.stringPropertyNames();
        ArrayList<TAConfig.RegionConfiguration> regionList = new ArrayList<TAConfig.RegionConfiguration>();
        for (String regionName : regionNames) {
            String wkt = properties.getProperty(regionName);
            Geometry geometry;
            try {
                geometry = wktReader.read(wkt);
            } catch (ParseException e) {
                throw new IllegalStateException(regionName + " = " + wkt, e);
            }
            regionList.add(new TAConfig.RegionConfiguration(regionName, geometry));
        }
        return regionList;
    }

    private static Properties loadRegions(String resource) {
        Properties properties = new Properties();
        InputStream inputStream = TAProductionType.class.getResourceAsStream(resource);
        if (inputStream == null) {
            throw new IllegalStateException("Resource not found: " + resource);
        }
        try {
            try {
                properties.load(inputStream);
            } finally {
                inputStream.close();
            }
            return properties;
        } catch (IOException e) {
            throw new IllegalStateException("Error reading resource: " + resource, e);
        }
    }


}
