package com.bc.calvalus.inventory.hadoop;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.local.LocalConfigKeys;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class CalvalusShConcreteFileSystem extends DelegateToFileSystem {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public CalvalusShConcreteFileSystem(URI uri, Configuration conf) throws URISyntaxException, IOException {
        super(uri, CalvalusShFileSystem.getCreate(uri, conf), conf, "file", false);
        LOG.info("new CalvalusShConcreteFileSystem for " + uri.toString() + " with " + fsImpl.getClass().getName());
    }

    @Override
    public int getUriDefaultPort() {
      return -1; // No default port for file:///
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
      return LocalConfigKeys.getServerDefaults();
    }

    @Override
    public boolean isValidName(String src) {
      // Different local file systems have different validation rules. Skip
      // validation here and just let the OS handle it. This is consistent with
      // RawLocalFileSystem.
      return true;
    }
}
