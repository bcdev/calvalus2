package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @author Marco Peters
 */
public class IOUtils {

    public static InputStream getInputStream(String url) throws IOException {
        InputStream inputStream;
        if (url.startsWith("hdfs:")) {
            final Configuration conf = new Configuration();
            final Path path = new Path(url);
            inputStream = path.getFileSystem(conf).open(path);
        } else {
            inputStream = new URL(url).openStream();
        }
        return inputStream;
    }
}
