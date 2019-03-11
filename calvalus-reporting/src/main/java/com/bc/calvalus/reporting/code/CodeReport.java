package com.bc.calvalus.reporting.code;

import com.bc.calvalus.reporting.common.UsageStatistic;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Locale;

/**
 * @author muhammad.bc.
 * @author hans
 */
public class CodeReport {

    private static final String PRODUCT_PROCESSED_MESSAGE = "ProductProcessedMessage";
    private static final String CODE_DE_PROCESSING_SERVICE = "code-de-processing-service";
    private static final String VERSION = "1.0";
    private static final String SERVICE_HOST = "processing";
    private static final String CALVALUS_PROCESSING_CENTER = "Calvalus";
    private static final double KILO_BYTE = 1024.0;
    private static final double SECONDS_PER_HOUR = 3600.0;
    private static final double MILLIS_PER_SECOND = 1000.0;

    private final String requestId;
    private final String jobName;
    private final String jobSubmissionTime;
    private final String userName;
    private final String queueName;
    private final String inProducts;
    private final String inProductsType;
    private final String inCollection;
    private final long inProductsNumber;
    private final double inProductsSize;
    private final String requestSource;
    private final String processingCenter;
    private final long configuredCpuCoresPerTask;
    private final double cpuCoreHours;
    private final String processorName;
    private final String processorDescription;
    private final double configuredRamPerTask;
    private final double ramHours;
    private final String processingWorkflow;
    private final double duration;
    private final String processingStatus;
    private final long outProductsNumber;
    private final String outProductsType;
    private final String outCollection;
    private final String outProductsLocation;
    private final double outProductsSize;

    private String messageType = PRODUCT_PROCESSED_MESSAGE;
    private String serviceId = CODE_DE_PROCESSING_SERVICE;
    private String serviceHost;
    private String messageTime;
    private String version = VERSION;

    CodeReport(
                String requestId,
                String jobName,
                String jobSubmissionTime,
                String userName,
                String queueName,
                String inProducts,
                String inProductsType,
                String inCollection,
                long inProductsNumber,
                double inProductsSize,
                String requestSource,
                String processingCenter,
                long configuredCpuCoresPerTask,
                double cpuCoreHours,
                String processorName,
                String processorDescription,
                double configuredRamPerTask,
                double ramHours,
                String processingWorkflow,
                double duration,
                String processingStatus,
                long outProductsNumber,
                String outProductsType,
                String outCollection,
                String outProductsLocation,
                double outProductsSize) {

        this.requestId = requestId;
        this.jobName = jobName;
        this.jobSubmissionTime = jobSubmissionTime;
        this.userName = userName;
        this.queueName = queueName;
        this.inProducts = inProducts;
        this.inProductsType = inProductsType;
        this.inCollection = inCollection;
        this.inProductsNumber = inProductsNumber;
        this.inProductsSize = inProductsSize;
        this.requestSource = requestSource;
        this.processingCenter = processingCenter;
        this.configuredCpuCoresPerTask = configuredCpuCoresPerTask;
        this.cpuCoreHours = cpuCoreHours;
        this.processorName = processorName;
        this.processorDescription = processorDescription;
        this.configuredRamPerTask = configuredRamPerTask;
        this.ramHours = ramHours;
        this.processingWorkflow = processingWorkflow;
        this.duration = duration;
        this.processingStatus = processingStatus;
        this.outProductsNumber = outProductsNumber;
        this.outProductsType = outProductsType;
        this.outCollection = outCollection;
        this.outProductsLocation = outProductsLocation;
        this.outProductsSize = outProductsSize;

        this.serviceHost = getHostName();
        this.messageTime = LocalDateTime.now().toString();
    }

    CodeReport(UsageStatistic usageStatistic) {
        this.requestId = usageStatistic.getJobId();
        this.jobName = usageStatistic.getJobName();
        this.jobSubmissionTime = convertMillisToIsoString(usageStatistic.getSubmitTime());
        this.userName = usageStatistic.getUser();
        this.queueName = usageStatistic.getQueue();
        this.inProducts = usageStatistic.getInputPath();
        this.inProductsType = usageStatistic.getInProductType();
        this.inCollection = usageStatistic.getCollectionName();
        this.inProductsNumber = usageStatistic.getTotalMaps();
        this.inProductsSize = getFileBytesRead(usageStatistic.getFileBytesRead(),
                                               usageStatistic.getInputFileBytesRead(),
                                               usageStatistic.getFileSplitBytesRead());
        this.requestSource = usageStatistic.getSystemName();
        this.processingCenter = CALVALUS_PROCESSING_CENTER;
        this.configuredCpuCoresPerTask = parseLong(usageStatistic.getConfiguredCpuCores());
        this.cpuCoreHours = usageStatistic.getCpuMilliseconds() / (SECONDS_PER_HOUR * MILLIS_PER_SECOND);
        this.processorName = resolveProcessorName(usageStatistic.getProcessType(),
                                                  usageStatistic.getMapClass(),
                                                  usageStatistic.getWorkflowType());
        this.processorDescription = usageStatistic.getProcessorDescription();
        this.configuredRamPerTask = parseLong(usageStatistic.getConfiguredRam()) / KILO_BYTE;
        this.ramHours = calculateRamHours(usageStatistic.getMbMillisMapTotal(),
                                          usageStatistic.getMbMillisReduceTotal());
        this.processingWorkflow = usageStatistic.getWorkflowType();
        this.duration = (usageStatistic.getFinishTime() - usageStatistic.getStartTime()) / MILLIS_PER_SECOND;
        this.processingStatus = usageStatistic.getState();
        this.outProductsNumber = usageStatistic.getReducesCompleted() > 0 ? usageStatistic.getReducesCompleted() : usageStatistic.getMapsCompleted();
        this.outProductsType = usageStatistic.getOutputType();
        this.outCollection = usageStatistic.getJobName();
        this.outProductsLocation = usageStatistic.getOutputDir() != null ?
                                   usageStatistic.getOutputDir() :
                                   resolveOutProductsLocation(usageStatistic.getWorkflowType(),
                                                              usageStatistic.getInputPath());
        this.outProductsSize = getGbFromBytes(usageStatistic.getFileBytesWritten());

        this.serviceHost = getHostName();
        this.messageTime = LocalDateTime.now().toString();
    }

    private void validateMandatoryFields(String mandatoryFields) {
        String[] split = mandatoryFields.split(",");
        for (String s : split) {
            try {
                Field declaredField = this.getClass().getDeclaredField(s);
                if (declaredField.get(this) == null) {
                    throw new IllegalArgumentException("Field '" + s + "' is mandatory but not available.");
                }
            } catch (NoSuchFieldException | IllegalAccessException exception) {
                throw new IllegalArgumentException(exception);
            }
        }
    }

    private String resolveOutProductsLocation(String workflowType, String inputPath) {
        if ("GeoDB".equalsIgnoreCase(workflowType)) {
            return inputPath;
        } else {
            return "Not available";
        }
    }

    private String resolveProcessorName(String processType, String mapClass, String workflowType) {
        if (processType != null) {
            return processType;
        } else if (mapClass.contains("l2.L2FormattingMapper") || mapClass.contains("l3.L3FormatterMapper")) {
            return "Formatting";
        } else if ("L3".equalsIgnoreCase(workflowType)) {
            return "Aggregation";
        } else if ("GeoDB".equalsIgnoreCase(workflowType)) {
            return "Geo Indexing";
        } else {
            return "None";
        }
    }

    private double getFileBytesRead(long fileBytesRead, long inputFileBytesRead, long fileSplitBytesRead) {
        if (fileSplitBytesRead > 0) {
            return getGbFromBytes(fileBytesRead + fileSplitBytesRead);
        } else if (inputFileBytesRead > 0) {
            return getGbFromBytes(fileBytesRead + inputFileBytesRead);
        } else {
            return getGbFromBytes(fileBytesRead);
        }
    }

    private long parseLong(String valueString) {
        try {
            return Long.parseLong(valueString);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private double getGbFromBytes(long fileBytesRead) {
        return fileBytesRead / (KILO_BYTE * KILO_BYTE * KILO_BYTE);
    }

    private String convertMillisToIsoString(long timeMillis) {
        Date date = new Date(timeMillis);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ENGLISH);
        return dateFormat.format(date);
    }

    private double calculateRamHours(long mbMillisMapTotal, long mbMillisReduceTotal) {
        return (mbMillisMapTotal + mbMillisReduceTotal) / (KILO_BYTE * SECONDS_PER_HOUR * MILLIS_PER_SECOND);
    }

    public String toJson(String mandatoryFields) {
        validateMandatoryFields(mandatoryFields);

        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder = gsonBuilder.registerTypeAdapter(Double.class, new JsonSerializer<Double>() {
            @Override
            public JsonElement serialize(Double src, Type typeOfSrc, JsonSerializationContext context) {
                return new JsonPrimitive((new BigDecimal(src)).setScale(6, BigDecimal.ROUND_HALF_UP));
            }
        });
        Gson gson = gsonBuilder.setPrettyPrinting().create();
        return gson.toJson(this);
    }

    private String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return SERVICE_HOST;
        }
    }
}
