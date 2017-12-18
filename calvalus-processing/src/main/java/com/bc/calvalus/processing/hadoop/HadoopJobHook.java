package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.mapreduce.Job;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public interface HadoopJobHook {
    void beforeSubmit(Job job);
}
