package com.bc.calvalus.wps.localprocess;

import com.bc.wps.api.schema.ExecuteResponse;

/**
 * @author hans
 */
public interface Process {

    ProductionStatus processAsynchronous(ProcessBuilder processBuilder);

    ProductionStatus processSynchronous(ProcessBuilder processBuilder);

    ExecuteResponse createLineageAsyncExecuteResponse(ProductionStatus status, ProcessBuilder processBuilder);

    ExecuteResponse createLineageSyncExecuteResponse(ProductionStatus status, ProcessBuilder processBuilder);

}
