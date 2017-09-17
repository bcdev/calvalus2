package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.net.URI;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class FileSystemSetter {
    public static FileSystem addFileSystemForTesting(URI uri, Configuration conf, FileSystem fs) throws IOException {
        FileSystem.addFileSystemForTesting(uri, conf, fs);
        return fs;
    }
}
