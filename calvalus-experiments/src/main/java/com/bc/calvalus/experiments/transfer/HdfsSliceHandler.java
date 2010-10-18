package com.bc.calvalus.experiments.transfer;

import com.bc.calvalus.experiments.processing.LogFormatter;
import com.bc.childgen.ChildGeneratorImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SliceHandler that opens (and closes) a new HDFS file for each slice.
 * The files are named like the input file with the appendix .s<sliceindex> .
 *
 * @author Martin Boettcher
 */
public class HdfsSliceHandler implements ChildGeneratorImpl.SliceHandler {
    private final String destination;
    private final FileSystem hdfs;
    private ImageOutputStream out = null;


    private static final Logger LOG = Logger.getLogger("com.bc.calvalus");

    static {
        Handler[] handlers = LOG.getHandlers();
        for (Handler handler : handlers) {
            LOG.removeHandler(handler);
        }
        Handler handler = new ConsoleHandler();
        handler.setFormatter(new LogFormatter());
        LOG.addHandler(handler);
        LOG.setLevel(Level.ALL);
    }

    public HdfsSliceHandler(FileSystem hdfs, String destination) {
        this.hdfs = hdfs;
        this.destination = destination;
    }

    @Override
    public ImageOutputStream beginSlice(int sliceIndex, String productName, int firstLine, int lastLine) throws IOException {
        LOG.log(Level.INFO, MessageFormat.format("Processing fragment {0} (line {1} ... {2})",
                                                 sliceIndex, firstLine, lastLine));
        Path destPath = new Path(destination, productName + ".s" + sliceIndex);
        out = new MemoryCacheImageOutputStream(hdfs.create(destPath, true));
        return out;
    }

    @Override
    public void endSlice(int sliceIndex, String productName, long bytesWritten) throws IOException {
        LOG.log(Level.INFO, MessageFormat.format("Fragment {0} processed, bytes written: {1}",
                                                 sliceIndex, bytesWritten));
//        out.close();
    }

    @Override
    public boolean handleError(int sliceIndex, IOException e) {
        LOG.log(Level.SEVERE, MessageFormat.format("Problem while processing fragment {0}",
                                                   sliceIndex), e);
        return true;
    }
}
