package com.bc.calvalus.portal.server.hadoop;

import com.bc.calvalus.portal.server.PortalConfig;
import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceException;
import com.bc.calvalus.portal.shared.PortalParameter;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.PortalProductionStatus;
import com.bc.calvalus.processing.beam.BeamJobService;
import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.processing.beam.StreamingProductReader;
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
import org.apache.velocity.app.VelocityEngine;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import static java.lang.Math.PI;

/**
 * An BackendService implementation that delegates to a Hadoop cluster.
 * To use it, specify the servlet init-parameter 'calvalus.portal.backendService.class'
 * (context.xml or web.xml)
 */
public class HadoopBackendService implements BackendService {

    public static final File PRODUCTIONS_DB_FILE = new File("calvalus-productions-db.csv");
    private static final int HADOOP_OBSERVATION_PERIOD = 2000;
    private final ServletContext servletContext;
    private final JobClient jobClient;
    private final HadoopProductionDatabase database;
    private final VelocityEngine velocityEngine;
    // todo - Persist
    private static long outputFileNum = 0;

    public HadoopBackendService(ServletContext servletContext) throws ServletException, IOException {
        this.servletContext = servletContext;
        this.database = new HadoopProductionDatabase();
        this.velocityEngine = new VelocityEngine();
        this.jobClient = new JobClient(createJobConf(servletContext));

        initVelocityEngine();
        initDatabase();

        // Prevent Windows from using ';' as path separator
        System.setProperty("path.separator", ":");

        Timer hadoopObservationTimer = new Timer(true);
        hadoopObservationTimer.scheduleAtFixedRate(new HadoopObservationTask(),
                                                   HADOOP_OBSERVATION_PERIOD / 2,
                                                   HADOOP_OBSERVATION_PERIOD);
    }

    @Override
    public PortalProductSet[] getProductSets(String filter) throws BackendServiceException {
        // todo - load & update from persistent storage
        return new PortalProductSet[]{
                new PortalProductSet("MER_RR__1P/r03/", "MERIS_RR__1P", "All MERIS RR L1b"),
                new PortalProductSet("MER_RR__1P/r03/2004", "MERIS_RR__1P", "MERIS RR L1b 2004"),
                new PortalProductSet("MER_RR__1P/r03/2005", "MERIS_RR__1P", "MERIS RR L1b 2005"),
                new PortalProductSet("MER_RR__1P/r03/2006", "MERIS_RR__1P", "MERIS RR L1b 2006"),
        };
    }

    @Override
    public PortalProcessor[] getProcessors(String filter) throws BackendServiceException {
        // todo - load & update from persistent storage
        return new PortalProcessor[]{
                new PortalProcessor("CoastColour.L2W", "MERIS CoastColour",
                                    "<parameters>\n" +
                                            "  <useIdepix>true</useIdepix>\n" +
                                            "  <landExpression>l1_flags.LAND_OCEAN</landExpression>\n" +
                                            "  <outputReflec>false</outputReflec>\n" +
                                            "</parameters>",
                                    "beam-lkn", new String[]{"1.0-SNAPSHOT"}),
        };
    }

    @Override
    public PortalProduction[] getProductions(String filter) throws BackendServiceException {
        HadoopProduction[] hadoopProductions = database.getProductions();
        PortalProduction[] portalProductions = new PortalProduction[hadoopProductions.length];
        for (int i = 0; i < hadoopProductions.length; i++) {
            HadoopProduction hadoopProduction = hadoopProductions[i];
            portalProductions[i] = new PortalProduction(hadoopProduction.getId(),
                                                        hadoopProduction.getName(),
                                                        hadoopProduction.getOutputPath(),
                                                        getPortalProductionStatus(hadoopProduction.getJobStatus()));
        }
        return portalProductions;
    }

    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest productionRequest) throws BackendServiceException {
        String productionType = productionRequest.getProductionType();
        Map<String, String> productionParameters = getProductionParametersMap(productionRequest);
        if ("calvalus-level3".equals(productionType)) {
            return orderL3Production(productionType, productionParameters);
        } else {
            throw new BackendServiceException(String.format("Unhandled production type '%s'", productionType));
        }
    }

    @Override
    public void cancelProductions(String[] productionIds) throws BackendServiceException {
        requestProductionKill(productionIds, HadoopProduction.Action.CANCEL);
    }

    @Override
    public void deleteProductions(String[] productionIds) throws BackendServiceException {
        requestProductionKill(productionIds, HadoopProduction.Action.DELETE);
    }

    private void requestProductionKill(String[] productionIds, HadoopProduction.Action action) throws BackendServiceException {
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
            throw new BackendServiceException(String.format("Only %d of %d production(s) have been deleted.", count, productionIds.length));
        }
    }

    public String stageProductionOutput(String productionId) throws BackendServiceException {
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

            File downloadDir = new File(new PortalConfig(servletContext).getLocalDownloadDir(),
                                        outputPath.getName());
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
            throw new BackendServiceException("Error: " + e.getMessage(), e);
        }

    }

    private PortalProductionResponse orderL3Production(String productionType, Map<String, String> productionParameters) throws BackendServiceException {
        String productionId = Long.toHexString(System.nanoTime());
        String productionName = String.format("%s using product set '%s' and L2 processor '%s'",
                                              productionType,
                                              productionParameters.get("inputProductSetId"),
                                              productionParameters.get("l2OperatorName"));
        String outputDir = getOutputDir(productionType,
                                        productionParameters);
        String wpsXml = createL3WpsXml(productionId,
                                       productionName,
                                       productionParameters,
                                       outputDir);
        JobID jobId = submitL3Job(wpsXml);
        HadoopProduction hadoopProduction = new HadoopProduction(productionId,
                                                                 productionName,
                                                                 outputDir,
                                                                 jobId);
        database.addProduction(hadoopProduction);
        return new PortalProductionResponse(new PortalProduction(hadoopProduction.getId(),
                                                                 hadoopProduction.getName(),
                                                                 hadoopProduction.getOutputPath(),
                                                                 new PortalProductionStatus(PortalProductionStatus.State.WAITING)));
    }

    private String createL3WpsXml(String productionId, String productionName, Map<String, String> productionParameters, String outputDir) throws BackendServiceException {
        String[] inputFiles;
        try {
            inputFiles = getInputFiles(productionParameters);
        } catch (IOException e) {
            throw new BackendServiceException("Failed to assemble input file list: " + e.getMessage(), e);
        }
        BeamL3Config.AggregatorConfiguration[] aggregators = getAggregators(productionParameters);

        VelocityContext context = new VelocityContext();
        context.put("productionId", productionId);
        context.put("productionName", productionName);
        context.put("processorPackage", "beam-lkn");
        context.put("processorVersion", "1.0-SNAPSHOT");
        context.put("inputFiles", inputFiles);
        context.put("outputDir", outputDir);
        context.put("l2OperatorName", productionParameters.get("l2OperatorName"));
        context.put("l2OperatorParameters", productionParameters.get("l2OperatorParameters"));
        context.put("superSampling", productionParameters.get("superSampling"));
        context.put("maskExpr", productionParameters.get("validMask"));
        context.put("numRows", getNumRows(productionParameters));
        context.put("bbox", getBBOX(productionParameters));
        context.put("variables", new BeamL3Config.VariableConfiguration[0]);
        context.put("aggregators", aggregators);

        String wpsXml;
        try {
            Template wpsXmlTemplate = velocityEngine.getTemplate("com/bc/calvalus/portal/server/level3-wps-request.xml.vm");
            StringWriter writer = new StringWriter();
            wpsXmlTemplate.merge(context, writer);
            wpsXml = writer.toString();
            System.out.println(wpsXml);
        } catch (Exception e) {
            throw new BackendServiceException("Failed to generate WPS XML request", e);
        }
        return wpsXml;
    }

    private String getOutputDir(String productionType, Map<String, String> productionParameters) {
        return productionParameters.get("outputFileName")
                .replace("${user}", System.getProperty("user.name", "hadoop"))
                .replace("${type}", productionType)
                .replace("${num}", (++outputFileNum) + "");
    }

    private int getNumRows(Map<String, String> productionParameters) {
        return getNumRows(Double.parseDouble(productionParameters.get("resolution")));
    }

    private String getBBOX(Map<String, String> productionParameters) {
        return String.format("%s,%s,%s,%s",
                             productionParameters.get("lonMin"), productionParameters.get("latMin"),
                             productionParameters.get("lonMax"), productionParameters.get("latMax"));
    }

    private BeamL3Config.AggregatorConfiguration[] getAggregators(Map<String, String> productionParameters) {
        String[] inputVariables = productionParameters.get("inputVariables").split(",");
        BeamL3Config.AggregatorConfiguration[] aggregatorConfigurations = new BeamL3Config.AggregatorConfiguration[inputVariables.length];
        for (int i = 0; i < inputVariables.length; i++) {
            aggregatorConfigurations[i] = new BeamL3Config.AggregatorConfiguration(productionParameters.get("aggregator"),
                                                                                   inputVariables[i],
                                                                                   Double.parseDouble(productionParameters.get("weightCoeff")));
        }
        return aggregatorConfigurations;
    }

    private JobID submitL3Job(String wpsXml) throws BackendServiceException {
        try {
            BeamJobService beamJobService = new BeamJobService(jobClient);
            return beamJobService.submitJob(wpsXml);
        } catch (Exception e) {
            e.printStackTrace();
            throw new BackendServiceException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }

    private String[] getInputFiles(Map<String, String> productionParameters) throws IOException {
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
            for (FileStatus fileStatuse : fileStatuses) {
                inputFileList.add(fileStatuse.getPath().toString());
            }
        }
        return inputFileList.toArray(new String[inputFileList.size()]);
    }

    private static List<String> getDateList(Date start, Date stop, String prefix) {
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

    private Map<String, String> getProductionParametersMap(PortalProductionRequest productionRequest) {
        HashMap<String, String> productionParametersMap = new HashMap<String, String>();
        PortalParameter[] productionParameters = productionRequest.getProductionParameters();
        for (PortalParameter productionParameter : productionParameters) {
            productionParametersMap.put(productionParameter.getName(), productionParameter.getValue());
        }
        return productionParametersMap;
    }

    private PortalProductionStatus getPortalProductionStatus(JobStatus job) {
        if (job != null) {
            // todo - use message that shows current 'action' value from 'HadoopProduction'
            float progress = 0.5f * (job.mapProgress() + job.reduceProgress());
            if (job.getRunState() == JobStatus.FAILED) {
                return new PortalProductionStatus(PortalProductionStatus.State.ERROR, progress);
            } else if (job.getRunState() == JobStatus.KILLED) {
                return new PortalProductionStatus(PortalProductionStatus.State.CANCELLED, progress);
            } else if (job.getRunState() == JobStatus.PREP) {
                return new PortalProductionStatus(PortalProductionStatus.State.WAITING, progress);
            } else if (job.getRunState() == JobStatus.RUNNING) {
                return new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS, progress);
            } else if (job.getRunState() == JobStatus.SUCCEEDED) {
                return new PortalProductionStatus(PortalProductionStatus.State.COMPLETED);
            }
        }
        return new PortalProductionStatus(PortalProductionStatus.State.UNKNOWN);
    }

    static int getNumRows(double res) {
        // see: SeaWiFS Technical Report Series Vol. 32;
        final double RE = 6378.145;
        int numRows = 1 + (int) Math.floor(0.5 * (2 * PI * RE) / res);
        if (numRows % 2 == 0) {
            return numRows;
        } else {
            return numRows + 1;
        }
    }

    private void initDatabase() {
        try {
            database.load(PRODUCTIONS_DB_FILE);
        } catch (IOException e) {
            servletContext.log("Failed to load productions: " + e.getMessage(), e);
        }
    }

    private static JobConf createJobConf(ServletContext servletContext) throws IOException {
        JobConf jobConf = new JobConf();
        Enumeration elements = servletContext.getInitParameterNames();
        while (elements.hasMoreElements()) {
            String name = (String) elements.nextElement();
            if (name.startsWith("calvalus.hadoop.")) {
                String hadoopName = name.substring("calvalus.hadoop.".length());
                String hadoopValue = servletContext.getInitParameter(name);
                jobConf.set(hadoopName, hadoopValue);
                // System.out.println("Using Hadoop configuration: " + hadoopName + " = " + hadoopValue);
            }
        }
        return jobConf;
    }

    private void initVelocityEngine() throws ServletException {
        Properties properties = new Properties();
        properties.setProperty("resource.loader", "class");
        properties.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        try {
            velocityEngine.init(properties);
        } catch (Exception e) {
            throw new ServletException(String.format("Failed to initialise Velocity engine: %s", e.getMessage()), e);
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
                servletContext.log("Failed to update production state:" + e.getMessage(), e);
                lastLog = time;
            }
        }
    }
}
