package com.bc.calvalus.processing.hadoop;


import org.apache.hadoop.conf.Configuration;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public interface HadoopJobHook {
    void beforeSubmit(Configuration job);
}
