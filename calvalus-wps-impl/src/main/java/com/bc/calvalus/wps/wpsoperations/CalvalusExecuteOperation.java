package com.bc.calvalus.wps.wpsoperations;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.ProcessFacade;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.localprocess.LocalFacade;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.DataInputsType;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.InputType;
import com.bc.wps.api.schema.ProcessBriefType;
import com.bc.wps.api.schema.ResponseDocumentType;
import com.bc.wps.api.schema.ResponseFormType;
import com.bc.wps.api.utils.WpsTypeConverter;
import org.esa.snap.core.gpf.GPF;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusExecuteOperation {

    private WpsRequestContext context;
    private final ProcessFacade processFacade;

    public CalvalusExecuteOperation(String processId, WpsRequestContext context) throws IOException {
        this.context = context;
        this.processFacade = getProcessFacade(processId);
    }

    private ProcessFacade getProcessFacade(String processId) throws IOException {
        if (processId.equals("urbantep-local~1.0~Subset")) {
            return new LocalFacade(context);
        } else {
            return new CalvalusFacade(context);
        }
    }

    public ExecuteResponse execute(Execute executeRequest)
                throws InvalidProcessorIdException, WpsProductionException,
                       WpsResultProductException, ProductionException, IOException {
        if(isQuotationRequest(executeRequest.getDataInputs())) {
            CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
            ExecuteResponse processBriefType1 = executeResponse.getQuotationResponse((String)this.processFacade.getRequestHeaderMap().get("remoteUser"),
                                                                                     (String)this.processFacade.getRequestHeaderMap().get("remoteRef"),
                                                                                     executeRequest.getDataInputs());
            ProcessBriefType responseFormType1 = getQuotationBriefType(executeRequest);
            processBriefType1.setProcess(responseFormType1);
            return processBriefType1;
        }
        ProcessBriefType processBriefType = getProcessBriefType(executeRequest);
        ResponseFormType responseFormType = executeRequest.getResponseForm();
        ResponseDocumentType responseDocumentType = responseFormType.getResponseDocument();
        boolean isAsynchronous = responseDocumentType.isStatus();
        boolean isLineage = responseDocumentType.isLineage();

        if (processFacade instanceof LocalFacade) {
            GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        }

        CalvalusExecuteResponseConverter executeResponse = new CalvalusExecuteResponseConverter();
        if (isAsynchronous) {
            LocalProductionStatus status = processFacade.orderProductionAsynchronous(executeRequest);
            ExecuteResponse asyncExecuteResponse;
            if (isLineage) {
                List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
                asyncExecuteResponse = executeResponse.getAcceptedWithLineageResponse(status.getJobId(),
                                                                                      executeRequest.getDataInputs(),
                                                                                      outputType,
                                                                                      context.getServerContext());
            } else {
                asyncExecuteResponse = executeResponse.getAcceptedResponse(status.getJobId(), context.getServerContext());
            }
            asyncExecuteResponse.setProcess(processBriefType);
            return asyncExecuteResponse;
        } else {
            LocalProductionStatus status = processFacade.orderProductionSynchronous(executeRequest);
            ExecuteResponse syncExecuteResponse;
            if (!ProcessState.COMPLETED.toString().equals(status.getState())) {
                syncExecuteResponse = executeResponse.getFailedResponse(status.getMessage());
            } else if (isLineage) {
                List<DocumentOutputDefinitionType> outputType = executeRequest.getResponseForm().getResponseDocument().getOutput();
                syncExecuteResponse = executeResponse.getSuccessfulWithLineageResponse(status.getResultUrls(),
                                                                                       executeRequest.getDataInputs(),
                                                                                       outputType);
            } else {
                syncExecuteResponse = executeResponse.getSuccessfulResponse(status.getResultUrls(), status.getStopTime());
            }
            syncExecuteResponse.setProcess(processBriefType);
            return syncExecuteResponse;
        }
    }

    private boolean isQuotationRequest(DataInputsType dataInputs) {
//        Iterator var2 = dataInputs.getInput().iterator();
//
//        InputType inputType;
//        do {
//            if(!var2.hasNext()) {
//                return false;
//            }
//
//            inputType = (InputType)var2.next();
//        } while(!"quotation".equalsIgnoreCase(inputType.getIdentifier().getValue()) || inputType.getData().getLiteralData() == null || !"true".equalsIgnoreCase(inputType.getData().getLiteralData().getValue()));
//
//        return true;
        for (InputType inputType : dataInputs.getInput()) {
            if ("quotation".equalsIgnoreCase(inputType.getIdentifier().getValue())) {
                if (inputType.getData().getLiteralData() != null && "true".equalsIgnoreCase(inputType.getData().getLiteralData().getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

    private ProcessBriefType getQuotationBriefType(Execute executeRequest) {
        ProcessBriefType processBriefType = new ProcessBriefType();
        processBriefType.setIdentifier(executeRequest.getIdentifier());
        processBriefType.setTitle(WpsTypeConverter.str2LanguageStringType(executeRequest.getIdentifier().getValue()));
        processBriefType.setProcessVersion("1.0");
        return processBriefType;
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
