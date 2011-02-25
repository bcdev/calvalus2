package com.bc.calvalus.portal.server;

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
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.net.NetUtils;
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
import java.net.InetSocketAddress;
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

import static java.lang.Math.*;

/**
 * An BackendService implementation that delegates to a Hadoop cluster.
 * To use it, specify the servlet init-parameter 'calvalus.portal.backendService.class'
 * (context.xml or web.xml)
 */
public class HadoopBackendService implements BackendService {

    private final ServletContext servletContext;
    private final Configuration hadoopConf;
    private final JobClient jobClient;
    private ArrayList<HadoopProduction> productions;
    private VelocityEngine velocityEngine;

    public HadoopBackendService(ServletContext servletContext) throws ServletException, IOException {
        this.servletContext = servletContext;
        this.hadoopConf = new Configuration();
        initVelocityEngine();
        initHadoopConf();
        String target = hadoopConf.get("mapred.job.tracker", "localhost:9001");
        InetSocketAddress trackerAddr = NetUtils.createSocketAddr(target);
        this.jobClient = new JobClient(trackerAddr, hadoopConf);
        productions = new ArrayList<HadoopProduction>();
    }

    private void initHadoopConf() {
        hadoopConf.reloadConfiguration();
        Enumeration elements = servletContext.getInitParameterNames();
        while (elements.hasMoreElements()) {
            String name = (String) elements.nextElement();
            if (name.startsWith("calvalus.hadoop.")) {
                String hadoopName = name.substring("calvalus.hadoop.".length());
                String hadoopValue = servletContext.getInitParameter(name);
                hadoopConf.set(hadoopName, hadoopValue);
                System.out.println("Using Hadoop configuration: " + hadoopName + " = " + hadoopValue);
            }
        }
    }

    private void initVelocityEngine() throws ServletException {
        Properties properties = new Properties();
        properties.setProperty("resource.loader", "class");
        properties.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        velocityEngine = new VelocityEngine();
        try {
            velocityEngine.init(properties);
        } catch (Exception e) {
            throw new ServletException(String.format("Failed to initialise Velocity engine: %s", e.getMessage()), e);
        }
    }

    @Override
    public PortalProductSet[] getProductSets(String filter) throws BackendServiceException {
        return new PortalProductSet[]{
                new PortalProductSet("MER_RR__1P/r03/", "MERIS_RR__1P", "All MERIS RR L1b"),
                new PortalProductSet("MER_RR__1P/r03/2004", "MERIS_RR__1P", "MERIS RR L1b 2004"),
                new PortalProductSet("MER_RR__1P/r03/2005", "MERIS_RR__1P", "MERIS RR L1b 2005"),
                new PortalProductSet("MER_RR__1P/r03/2006", "MERIS_RR__1P", "MERIS RR L1b 2006"),
        };
    }

    @Override
    public PortalProcessor[] getProcessors(String filter) throws BackendServiceException {
        return new PortalProcessor[]{
                new PortalProcessor("CoastColour.L2W", "MERIS CoastColour", "beam-lkn", new String[]{"1.0-SNAPSHOT"}),
        };
    }

    @Override
    public PortalProduction[] getProductions(String filter) throws BackendServiceException {
        try {
            // todo - this is still wrong, must first load all (persistent) productions,
            //        then retrieve its Hadoop queue-ID and then call jobClient.getJobsFromQueue();
            JobStatus[] jobStatuses = jobClient.getAllJobs();
            ArrayList<PortalProduction> productions = new ArrayList<PortalProduction>();
            for (int i = 0; i < jobStatuses.length; i++) {
                JobStatus jobStatus = jobStatuses[i];
                RunningJob runningJob = jobClient.getJob(jobStatus.getJobID());
                String productionId = jobStatus.getJobID().toString();
                // System.out.printf("Production %d: %s (id=%s)%n", (i + 1), runningJob.getJobName(), productionId);
                productions.add(new PortalProduction(productionId,
                                                     runningJob.getJobName(),
                                                     getPortalProductionStatus(jobStatus)));
            }
            return productions.toArray(new PortalProduction[productions.size()]);
        } catch (IOException e) {
            throw new BackendServiceException("Failed to retrieve job list from Hadoop.", e);
        }
    }

    static long outputFileNum = 0;

    @Override
    public PortalProductionResponse orderProduction(PortalProductionRequest productionRequest) throws BackendServiceException {
        String productionType = productionRequest.getProductionType();
        if (!"calvalus-level3".equals(productionType)) {
            return new PortalProductionResponse(1, "Unhandled production type '" + productionType + "'");
        }

        Map<String, String> productionParameters = getProductionParametersMap(productionRequest);
        String[] inputFiles;
        try {
            inputFiles = getInputFiles(productionParameters);
        } catch (IOException e) {
            throw new BackendServiceException("Failed to assemble input file list: " + e.getMessage(), e);
        }

        String inputProductSetId = productionParameters.get("inputProductSetId");
        String l2OperatorName = productionParameters.get("l2OperatorName");

        String productionName = "L3(L2(" + inputProductSetId + ", " + l2OperatorName + "))";
        String productionId = productionName + "-" + Long.toHexString(System.nanoTime());

        BeamL3Config.AggregatorConfiguration[] aggregators = getAggregators(productionParameters);

        VelocityContext context = new VelocityContext();
        context.put("productionId", productionId);
        context.put("processorPackage", "beam-lkn");
        context.put("processorVersion", "1.0-SNAPSHOT");
        context.put("outputDir", "output-" + productionId);
        context.put("inputFiles", inputFiles);
        context.put("outputDir", getOutputDir(productionType, productionParameters));
        context.put("l2OperatorName", productionParameters.get("l2OperatorName"));
        context.put("l2OperatorParameters", productionParameters.get("l2OperatorParameters"));
        context.put("superSampling", productionParameters.get("superSampling"));
        context.put("maskExpr", productionParameters.get("maskExpr"));
        context.put("numRows", getNumRows(productionParameters));
        context.put("bbox", getBBOX(productionParameters));
        context.put("variables", new BeamL3Config.VariableConfiguration[0]);
        context.put("aggregators", aggregators);


        // todo - add l3 params to context (bbox, aggegators ...)

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

        Job job = submitJob(wpsXml);

        HadoopProduction hadoopProduction = new HadoopProduction(productionId, productionName, job);
        productions.add(hadoopProduction);

        return new PortalProductionResponse(new PortalProduction(hadoopProduction.getId(),
                                                                 hadoopProduction.getName(),
                                                                 new PortalProductionStatus(PortalProductionStatus.State.WAITING,
                                                                                            "Hadoop job submitted.", 0.0f)));
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

    private Job submitJob(String wpsXml) throws BackendServiceException {
        Job job;
        try {
            BeamJobService beamJobService = new BeamJobService();
            job = beamJobService.createBeamHadoopJob(hadoopConf, wpsXml);
            job.submit();
        } catch (Exception e) {
            throw new BackendServiceException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
        return job;
    }

    private String[] getInputFiles(Map<String, String> productionParameters) throws IOException {
        Path eoDataRoot = new Path("hdfs://cvmaster00:9000/calvalus/eodata/");
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
        FileSystem fileSystem = inputPath.getFileSystem(hadoopConf);
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

    @Override
    public boolean[] cancelProductions(String[] productionIds) throws BackendServiceException {
        try {
            boolean[] result = new boolean[productionIds.length];
            JobStatus[] jobsStatuses = jobClient.getAllJobs();
            for (int i = 0; i < productionIds.length; i++) {
                JobStatus jobStatus = findJobStatus(jobsStatuses, productionIds[i]);
                if (jobsStatuses != null) {
                    RunningJob job = jobClient.getJob(jobStatus.getJobID());
                    job.killJob();
                    result[i] = true;
                }
            }
            return result;
        } catch (IOException e) {
            throw new BackendServiceException("Failed to cancel jobs", e);
        }
    }

    @Override
    public boolean[] deleteProductions(String[] productionIds) throws BackendServiceException {
        // todo - delete productions as well.
        return cancelProductions(productionIds);
    }

    @Override
    public String stageProductionOutput(String productionId) throws BackendServiceException {
        // todo - spawn separate thread, use StagingRequest/StagingResponse/WorkStatus
        try {
            RunningJob job = jobClient.getJob(org.apache.hadoop.mapred.JobID.forName(productionId));
            String jobFile = job.getJobFile();
            System.out.printf("jobFile = %n%s%n", jobFile);
            Configuration configuration = new Configuration(hadoopConf);
            configuration.addResource(new Path(jobFile));

            String jobOutputDir = configuration.get("mapred.output.dir");
            System.out.println("mapred.output.dir = " + jobOutputDir);
            Path outputPath = new Path(jobOutputDir);
            FileSystem fileSystem = outputPath.getFileSystem(hadoopConf);
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
                StreamingProductReader reader = new StreamingProductReader(seqProductPath, hadoopConf);
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

    private static JobStatus findJobStatus(JobStatus[] jobsStatuses, String productionId) {
        JobID jobID = JobID.forName(productionId);
        for (JobStatus jobStatus : jobsStatuses) {
            if (jobStatus.getJobID().equals(jobID)) {
                return jobStatus;
            }
        }
        return null;
    }

    private PortalProductionStatus getPortalProductionStatus(JobStatus job) throws IOException {
        if (job != null) {
            float progress = (job.setupProgress() + job.cleanupProgress() + job.mapProgress() + job.reduceProgress()) / 4.0f;
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


    public static int getNumRows(double res) {
        // see: SeaWiFS Technical Report Series Vol. 32;
        final double RE = 6378.145;
        int numRows = 1 + (int) Math.floor(0.5 * (2 * PI * RE) / res);
        if (numRows % 2 == 0) {
            return numRows;
        } else {
            return numRows + 1;
        }
    }
}
