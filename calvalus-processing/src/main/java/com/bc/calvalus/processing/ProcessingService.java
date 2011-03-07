package com.bc.calvalus.processing;

/**
 */
public interface ProcessingService {
    Object submitJob(Object args);
    Object[] getAllJobStatuses();
}
