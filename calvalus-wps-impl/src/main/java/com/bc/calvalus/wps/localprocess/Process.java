package com.bc.calvalus.wps.localprocess;

import com.bc.wps.api.schema.ExecuteResponse;

/**
 * @author hans
 */
public interface Process {

    LocalProductionStatus processAsynchronous(ProcessBuilder processBuilder);

    LocalProductionStatus processSynchronous(ProcessBuilder processBuilder);

    ExecuteResponse createLineageAsyncExecuteResponse(LocalProductionStatus status, ProcessBuilder processBuilder);

    ExecuteResponse createLineageSyncExecuteResponse(LocalProductionStatus status, ProcessBuilder processBuilder);

}
