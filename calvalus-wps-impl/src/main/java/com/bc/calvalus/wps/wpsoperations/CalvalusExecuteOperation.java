package com.bc.calvalus.wps.wpsoperations;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wps.calvalusfacade.CalvalusProcessor;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.localprocess.GpfProductionService;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.localprocess.Process;
import com.bc.calvalus.wps.localprocess.ProcessBuilder;
import com.bc.calvalus.wps.localprocess.ProductionState;
import com.bc.calvalus.wps.localprocess.SubsettingProcess;
import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
import com.bc.wps.api.exceptions.MissingParameterValueException;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ProcessBriefType;
import com.bc.wps.api.schema.ResponseDocumentType;
import com.bc.wps.api.schema.ResponseFormType;
import com.bc.wps.api.utils.WpsTypeConverter;
import com.bc.wps.utilities.PropertiesWrapper;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
public class CalvalusExecuteOperation extends WpsOperation {

    private static final String CATALINA_BASE = System.getProperty("catalina.base");
    private WpsRequestContext context;

    public CalvalusExecuteOperation(WpsRequestContext context) throws IOException {
        super(context);
        this.context = context;
    }

    public ExecuteResponse execute(Execute executeRequest)
                throws InvalidProcessorIdException, MissingParameterValueException, InvalidParameterValueException,
                       JAXBException, IOException, ProductionException, InterruptedException {
        ProcessBriefType processBriefType = getProcessBriefType(executeRequest);
        ResponseFormType responseFormType = executeRequest.getResponseForm();
        ResponseDocumentType responseDocumentType = responseFormType.getResponseDocument();
        boolean isAsynchronous = responseDocumentType.isStatus();
        boolean isLineage = responseDocumentType.isLineage();
        String processId = executeRequest.getIdentifier().getValue();

        if (processId.equals("urbantep-local~1.0~Subset")) {
            ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);
            Map<String, String> inputParameters = requestExtractor.getInputParametersMapRaw();

            GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

            final Product sourceProduct = getSourceProduct(inputParameters);
            String jobId = GpfProductionService.createJobId(context.getUserName());
            Path targetDirPath = getTargetDirectoryPath(jobId);
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("productionName", inputParameters.get("productionName"));
            parameters.put("geoRegion", inputParameters.get("regionWKT"));
            parameters.put("outputFormat", inputParameters.get("outputFormat"));
            parameters.put("productionType", inputParameters.get("productionType"));
            parameters.put("sourceProduct", inputParameters.get("sourceProduct"));
            parameters.put("copyMetadata", inputParameters.get("copyMetadata"));
            parameters.put("targetDir", targetDirPath.toString());
            CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            System.out.println("context.getServerContext().getRequestUrl() = " + context.getServerContext().getRequestUrl());
            ProcessBuilder processBuilder = ProcessBuilder.create()
                        .withJobId(jobId)
                        .withProcessId(processId)
                        .withParameters(parameters)
                        .withSourceProduct(sourceProduct)
                        .withTargetDirPath(targetDirPath)
                        .withServerContext(context.getServerContext())
                        .withExecuteRequest(executeRequest);
            Process utepProcess = new SubsettingProcess();

            if (isAsynchronous) {
                LocalProductionStatus status = utepProcess.processAsynchronous(processBuilder);
                if (isLineage) {
                    return utepProcess.createLineageAsyncExecuteResponse(status, processBuilder);
                }
                return executeResponse.getAcceptedResponse(status.getJobId(), context.getServerContext());
            } else {
                LocalProductionStatus status = utepProcess.processSynchronous(processBuilder);
                if (!ProductionState.SUCCESSFUL.toString().equals(status.getState())) {
                    return executeResponse.getFailedResponse(status.getMessage());
                }
                if (isLineage) {
                    return utepProcess.createLineageSyncExecuteResponse(status, processBuilder);
                }
                return executeResponse.getSuccessfulResponse(status.getResultUrls(), new Date());
            }
        } else if (isAsynchronous) {
            String jobId = processAsync(executeRequest, processId);
            ExecuteResponse asyncExecuteResponse = createAsyncExecuteResponse(executeRequest, isLineage, jobId);
            asyncExecuteResponse.setProcess(processBriefType);
            return asyncExecuteResponse;
        } else {
            String jobId = processSync(executeRequest, processId);
            ExecuteResponse syncExecuteResponse = createSyncExecuteResponse(executeRequest, isLineage, jobId);
            syncExecuteResponse.setProcess(processBriefType);
            return syncExecuteResponse;
        }
    }

    private Path getTargetDirectoryPath(String jobId) throws IOException {
        Path targetDirectoryPath = Paths.get(CATALINA_BASE + PropertiesWrapper.get("wps.application.path"),
                                             PropertiesWrapper.get("utep.output.directory"), calvalusFacade.getUserName(), jobId);
        Files.createDirectories(targetDirectoryPath);
        return targetDirectoryPath;
    }

    private Product getSourceProduct(Map<String, String> inputParameters) throws IOException {
        final Product sourceProduct;
        Path dir = Paths.get(CATALINA_BASE + PropertiesWrapper.get("wps.application.path"), PropertiesWrapper.get("utep.input.directory"));
        List<File> files = new ArrayList<>();
        DirectoryStream<Path> stream = Files.newDirectoryStream(dir, inputParameters.get("sourceProduct"));
        for (Path entry : stream) {
            files.add(entry.toFile());
        }

        String sourceProductPath;
        if (files.size() != 0) {
            sourceProductPath = files.get(0).getAbsolutePath();
        } else {
            throw new FileNotFoundException("The source product '" + inputParameters.get("sourceProduct") + "' cannot be found");
        }

        sourceProduct = ProductIO.readProduct(sourceProductPath);
        return sourceProduct;
    }

    String processSync(Execute executeRequest, String processorId)
                throws IOException, ProductionException, InvalidProcessorIdException,
                       JAXBException, InterruptedException, InvalidParameterValueException, MissingParameterValueException {
        ProductionRequest request = createProductionRequest(executeRequest, processorId);

        String jobid = calvalusFacade.orderProductionSynchronous(request);
        calvalusFacade.stageProduction(jobid);
        calvalusFacade.observeStagingStatus(jobid);
        return jobid;
    }

    String processAsync(Execute executeRequest, String processorId)
                throws IOException, ProductionException, InvalidProcessorIdException, JAXBException,
                       InvalidParameterValueException, MissingParameterValueException {
        ProductionRequest request = createProductionRequest(executeRequest, processorId);

        return calvalusFacade.orderProductionAsynchronous(request);
    }

    ExecuteResponse createAsyncExecuteResponse(Execute executeRequest, boolean isLineage, String productionId) {
        if (isLineage) {
            CalvalusExecuteResponseConverter executeAcceptedResponse = new CalvalusExecuteResponseConverter();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeAcceptedResponse.getAcceptedWithLineageResponse(productionId, executeRequest.getDataInputs(),
                                                                          outputType, context.getServerContext());
        } else {
            CalvalusExecuteResponseConverter executeAcceptedResponse = new CalvalusExecuteResponseConverter();
            return executeAcceptedResponse.getAcceptedResponse(productionId, context.getServerContext());
        }
    }

    ExecuteResponse createSyncExecuteResponse(Execute executeRequest, boolean isLineage, String jobId)
                throws IOException, ProductionException {
        Production production = calvalusFacade.getProduction(jobId);
        List<String> productResultUrls = calvalusFacade.getProductResultUrls(production);
        WorkflowItem workflowItem = production.getWorkflow();
        if (isLineage) {
            CalvalusExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
            List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
            return executeSuccessfulResponse.getSuccessfulWithLineageResponse(productResultUrls, executeRequest.getDataInputs(), outputType);
        } else {
            CalvalusExecuteResponseConverter executeSuccessfulResponse = new CalvalusExecuteResponseConverter();
            return executeSuccessfulResponse.getSuccessfulResponse(productResultUrls, workflowItem.getStopTime());
        }
    }

    private ProductionRequest createProductionRequest(Execute executeRequest, String processorId)
                throws JAXBException, IOException, ProductionException, InvalidProcessorIdException,
                       InvalidParameterValueException, MissingParameterValueException {
        ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(executeRequest);

        ProcessorNameConverter parser = new ProcessorNameConverter(processorId);
        CalvalusProcessor calvalusProcessor = calvalusFacade.getProcessor(parser);
        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, calvalusProcessor,
                                                                       calvalusFacade.getProductSets());

        return new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                     calvalusFacade.getUserName(),
                                     calvalusDataInputs.getInputMapFormatted());
    }

    private ProcessBriefType getProcessBriefType(Execute executeRequest) throws InvalidProcessorIdException {
        ProcessBriefType processBriefType = new ProcessBriefType();
        processBriefType.setIdentifier(executeRequest.getIdentifier());
        processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(executeRequest.getIdentifier().getValue()));
        ProcessorNameConverter parser = new ProcessorNameConverter(executeRequest.getIdentifier().getValue());
        processBriefType.setProcessVersion(parser.getBundleVersion());
        return processBriefType;
    }
}
