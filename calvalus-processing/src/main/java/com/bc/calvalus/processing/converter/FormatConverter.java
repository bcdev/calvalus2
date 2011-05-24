package com.bc.calvalus.processing.converter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public interface FormatConverter {

    void convert(String taskId, Path inputPath, String outputDir, String targetFormat, Configuration configuration)
        throws Exception;
}
