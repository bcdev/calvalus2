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
        FileSystem.Cache.Key key = new FileSystem.Cache.Key(uri, conf);
        FileSystem fs0 = FileSystem.CACHE.get(uri, conf);
        if (fs0 != null) {
            System.out.println("removing " + fs0 + " from cache to add " + fs);
            FileSystem.CACHE.remove(key, fs0);
        }
        FileSystem.addFileSystemForTesting(uri, conf, fs);
        return fs;
    }
}
