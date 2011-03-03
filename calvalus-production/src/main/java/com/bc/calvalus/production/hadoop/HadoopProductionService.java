package com.bc.calvalus.production.hadoop;


import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.processing.beam.BeamJobService;
import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.processing.beam.StreamingProductReader;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionProcessor;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.*;

/**
 * A ProductionService implementation that delegates to a Hadoop cluster.
 * To use it, specify the servlet init-parameter 'calvalus.portal.backendService.class'
 * (context.xml or web.xml)
 */
public class HadoopProductionService implements ProductionService {

    public static final File PRODUCTIONS_DB_FILE = new File("calvalus-productions-db.csv");
    private static final int HADOOP_OBSERVATION_PERIOD = 2000;
    private final JobClient jobClient;
    private final HadoopProductionDatabase database;
    private final StagingService stagingService;
    // todo - Persist
    private static long outputFileNum = 0;
    private final Logger logger;
    private final File stagingDirectory;
    private WpsXmlGenerator wpsXmlGenerator;

    public HadoopProductionService(JobConf jobConf, Logger logger, File stagingDirectory) throws ProductionException {
        this.logger = logger;
        this.stagingDirectory = stagingDirectory;
        this.database = new HadoopProductionDatabase();
        try {
            this.jobClient = new JobClient(jobConf);
        } catch (IOException e) {
            throw new ProductionException("Failed to create Hadoop JobClient." + e.getMessage(), e);
        }

        initDatabase();

        // Prevent Windows from using ';' as path separator
        System.setProperty("path.separator", ":");

        Timer hadoopObservationTimer = new Timer(true);
        hadoopObservationTimer.scheduleAtFixedRate(new HadoopObservationTask(),
                                                   HADOOP_OBSERVATION_PERIOD / 2,
                                                   HADOOP_OBSERVATION_PERIOD);
        stagingService = new StagingService(logger);
        wpsXmlGenerator = new WpsXmlGenerator();
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws ProductionException {
        // todo - load & update from persistent storage
        return new ProductSet[]{
                new ProductSet("MER_RR__1P/r03/", "MERIS_RR__1P", "All MERIS RR L1b"),
                new ProductSet("MER_RR__1P/r03/2004", "MERIS_RR__1P", "MERIS RR L1b 2004"),
                new ProductSet("MER_RR__1P/r03/2005", "MERIS_RR__1P", "MERIS RR L1b 2005"),
                new ProductSet("MER_RR__1P/r03/2006", "MERIS_RR__1P", "MERIS RR L1b 2006"),
        };
    }

    @Override
    public ProductionProcessor[] getProcessors(String filter) throws ProductionException {
        // todo - load & update from persistent storage
        return new ProductionProcessor[]{
                new ProductionProcessor("CoastColour.L2W", "MERIS CoastColour",
                                        "<parameters>\n" +
                                                "  <useIdepix>true</useIdepix>\n" +
                                                "  <landExpression>l1_flags.LAND_OCEAN</landExpression>\n" +
                                                "  <outputReflec>false</outputReflec>\n" +
                                                "</parameters>",
                                        "beam-lkn", new String[]{"1.0-SNAPSHOT"}),
        };
    }

    @Override
    public Production[] getProductions(String filter) throws ProductionException {
        return database.getProductions();
    }

    @Override
    public ProductionResponse orderProduction(ProductionRequest productionRequest) throws ProductionException {
        if ("calvalus-level3".equals(productionRequest.getProductionType())) {
            return orderL3Production(productionRequest);
        } else {
            throw new ProductionException(String.format("Unhandled production type '%s'",
                                                        productionRequest.getProductionType()));
        }
    }

    @Override
    public void cancelProductions(String[] productionIds) throws ProductionException {
        requestProductionKill(productionIds, HadoopProduction.Action.CANCEL);
    }

    @Override
    public void deleteProductions(String[] productionIds) throws ProductionException {
        requestProductionKill(productionIds, HadoopProduction.Action.DELETE);
    }

    private void requestProductionKill(String[] productionIds, HadoopProduction.Action action) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            HadoopProduction hadoopProduction = database.getProduction(productionId);
            if (hadoopProduction != null) {
                hadoopProduction.setAction(action);
                try {
                    JobID jobId = hadoopProduction.getJobId();
                    org.apache.hadoop.mapred.JobID oldJobId = org.apache.hadoop.mapred.JobID.downgrade(jobId);
                    RunningJob runningJob = jobClient.getJob(oldJobId);
                    if (runningJob != null) {
                        runningJob.killJob();
                        count++;
                    }
                } catch (IOException e) {
                    // nothing to do here
                }
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(String.format("Only %d of %d production(s) have been deleted.", count, productionIds.length));
        }
    }

    public String stageProductionOutput(String productionId) throws ProductionException {
        // todo - spawn separate thread, use StagingRequest/StagingResponse/WorkStatus
        try {
            RunningJob job = jobClient.getJob(org.apache.hadoop.mapred.JobID.forName(productionId));
            String jobFile = job.getJobFile();
            // System.out.printf("jobFile = %n%s%n", jobFile);
            Configuration configuration = new Configuration(jobClient.getConf());
            configuration.addResource(new Path(jobFile));

            String jobOutputDir = configuration.get("mapred.output.dir");
            // System.out.println("mapred.output.dir = " + jobOutputDir);
            Path outputPath = new Path(jobOutputDir);
            FileSystem fileSystem = outputPath.getFileSystem(jobClient.getConf());
            FileStatus[] seqFiles = fileSystem.listStatus(outputPath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().endsWith(".seq");
                }
            });

            File downloadDir = new File(stagingDirectory, outputPath.getName());
            if (!downloadDir.exists()) {
                downloadDir.mkdirs();
            }
            for (FileStatus seqFile : seqFiles) {
                Path seqProductPath = seqFile.getPath();
                System.out.println("seqProductPath = " + seqProductPath);
                StreamingProductReader reader = new StreamingProductReader(seqProductPath, jobClient.getConf());
                Product product = reader.readProductNodes(null, null);
                String dimapProductName = seqProductPath.getName().replaceFirst(".seq", ".dim");
                System.out.println("dimapProductName = " + dimapProductName);
                File productFile = new File(downloadDir, dimapProductName);
                ProductIO.writeProduct(product, productFile, ProductIO.DEFAULT_FORMAT_NAME, false);
            }
            // todo - zip or tar.gz all output DIMAPs to outputPath.getName() + ".zip" and remove outputPath.getName()
            return outputPath.getName() + ".zip";
        } catch (Exception e) {
            throw new ProductionException("Error: " + e.getMessage(), e);
        }

    }

    private ProductionResponse orderL3Production(ProductionRequest productionRequest) throws ProductionException {


        L3ProcessingRequest l3ProcessingRequest = new HadoopL3ProcessingRequest(jobClient, productionRequest);

        String l3ProductionId = createL3ProductionId(productionRequest);
        String l3ProductionName = createL3ProductionName(productionRequest);
        String wpsXml = wpsXmlGenerator.createL3WpsXml(l3ProductionId, l3ProductionName, l3ProcessingRequest);

        JobID jobId = submitL3Job(wpsXml);

        HadoopProduction hadoopProduction = new HadoopProduction(l3ProductionId,
                                                                 l3ProductionName,
                                                                 jobId,
                                                                 l3ProcessingRequest.getOutputDir(),
                                                                 l3ProcessingRequest.getOutputFormat(),
                                                                 l3ProcessingRequest.getOutputStaging());
        hadoopProduction.setWpsXml(wpsXml); // todo - check: this is only needed for staging (output directory)
        database.addProduction(hadoopProduction);
        return new ProductionResponse(hadoopProduction);
    }

    static String createL3ProductionId(ProductionRequest productionRequest) {
        return productionRequest.getProductionType() + "-" + Long.toHexString(System.nanoTime());

    }
    static String createL3ProductionName(ProductionRequest productionRequest) {
        return String.format("Level 3 production using product set '%s' and L2 processor '%s'",
                                              productionRequest.getProductionParameter("inputProductSetId"),
                                              productionRequest.getProductionParameter("l2ProcessorName"));

    }

    private JobID submitL3Job(String wpsXml) throws ProductionException {
        try {
            BeamJobService beamJobService = new BeamJobService(jobClient);
            return beamJobService.submitJob(wpsXml);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ProductionException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }

    String[] getInputFiles(Map<String, String> productionParameters) throws IOException {
        Path eoDataRoot = new Path(jobClient.getConf().get("fs.default.name"), "/calvalus/eodata/");
        DateFormat dateFormat = ProductData.UTC.createDateFormat("yyyy-MM-dd");
        Date startDate = null;
        try {
            startDate = dateFormat.parse(productionParameters.get("dateStart"));
        } catch (ParseException ignore) {
        }
        Date stopDate = null;
        try {
            stopDate = dateFormat.parse(productionParameters.get("dateStop"));
        } catch (ParseException ignore) {
        }
        String inputProductSetId = productionParameters.get("inputProductSetId");
        Path inputPath = new Path(eoDataRoot, inputProductSetId);
        List<String> dateList = getDateList(startDate, stopDate, inputProductSetId);
        FileSystem fileSystem = inputPath.getFileSystem(jobClient.getConf());
        List<String> inputFileList = new ArrayList<String>();
        for (String day : dateList) {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(eoDataRoot, day));
            for (FileStatus fileStatus : fileStatuses) {
                inputFileList.add(fileStatus.getPath().toString());
            }
        }
        return inputFileList.toArray(new String[inputFileList.size()]);
    }

    static List<String> getDateList(Date start, Date stop, String prefix) {
        Calendar startCal = ProductData.UTC.createCalendar();
        Calendar stopCal = ProductData.UTC.createCalendar();
        startCal.setTime(start);
        stopCal.setTime(stop);
        List<String> list = new ArrayList<String>();
        do {
            String dateString = String.format("MER_RR__1P/r03/%1$tY/%1$tm/%1$td", startCal);
            if (dateString.startsWith(prefix)) {
                list.add(dateString);
            }
            startCal.add(Calendar.DAY_OF_WEEK, 1);
        } while (!startCal.after(stopCal));

        return list;
    }

    private void initDatabase() {
        System.out.println("PRODUCTIONS_DB_FILE = " + PRODUCTIONS_DB_FILE.getAbsolutePath());
        try {
            database.load(PRODUCTIONS_DB_FILE);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to load productions: " + e.getMessage(), e);
        }
    }


    private Map<JobID, JobStatus> getJobStatusMap() throws IOException {
        JobStatus[] jobStatuses = jobClient.getAllJobs();
        HashMap<JobID, JobStatus> jobStatusMap = new HashMap<JobID, JobStatus>();
        for (JobStatus jobStatus : jobStatuses) {
            jobStatusMap.put(jobStatus.getJobID(), jobStatus);
        }
        return jobStatusMap;
    }

    private void updateProductionsState() throws IOException {
        Map<JobID, JobStatus> jobStatusMap = getJobStatusMap();
        HadoopProduction[] productions = database.getProductions();

        // Update state of all registered productions
        for (HadoopProduction production : productions) {
            production.setJobStatus(jobStatusMap.get(production.getJobId()));
        }

        // Now try to delete productions
        for (HadoopProduction production : productions) {
            if (HadoopProduction.Action.DELETE.equals(production.getAction())) {
                JobStatus jobStatus = production.getJobStatus();
                if (isDone(jobStatus)) {
                    database.removeProduction(production);
                }
            }
        }

        // copy result to staging area
        for (HadoopProduction production : productions) {
            if (production.getOutputStaging()
                    && production.getStagingStatus().getState() == ProductionState.UNKNOWN) {
                JobStatus jobStatus = production.getJobStatus();
                if (jobStatus.getRunState() == JobStatus.SUCCEEDED) {
                    production.setStagingStatus(new ProductionStatus(ProductionState.WAITING));
                    stagingService.stageProduction(production, jobClient.getConf());
                }
            }
        }

        // write to persistent storage
        database.store(PRODUCTIONS_DB_FILE);
    }

    private boolean isDone(JobStatus jobStatus) {
        return !isRunning(jobStatus);
    }

    private boolean isRunning(JobStatus jobStatus) {
        return jobStatus.getRunState() == JobStatus.PREP
                || jobStatus.getRunState() == JobStatus.RUNNING;
    }

    private class HadoopObservationTask extends TimerTask {
        private long lastLog;

        @Override
        public void run() {
            try {
                updateProductionsState();
            } catch (IOException e) {
                logError(e);
            }
        }

        private void logError(IOException e) {
            long time = System.currentTimeMillis();
            if (time - lastLog > 120 * 1000L) {
                logger.log(Level.SEVERE, "Failed to update production state:" + e.getMessage(), e);
                lastLog = time;
            }
        }
    }
}
