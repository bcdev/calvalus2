package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.wps.ProcessFacade;
import com.bc.wps.api.schema.ExecuteResponse;

/**
 * @author hans
 */
public interface Process {

    LocalProductionStatus processAsynchronous(ProcessFacade processFacade, ProcessBuilder processBuilder);

    LocalProductionStatus processSynchronous(ProcessFacade processFacade, ProcessBuilder processBuilder);

    ExecuteResponse createLineageAsyncExecuteResponse(LocalProductionStatus status, ProcessBuilder processBuilder);

    ExecuteResponse createLineageSyncExecuteResponse(LocalProductionStatus status, ProcessBuilder processBuilder);

}
