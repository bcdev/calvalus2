package com.bc.calvalus.experiments.transfer;

import com.bc.calvalus.experiments.format.FormatPerformanceMetrics;
import com.bc.calvalus.experiments.util.CalvalusLogger;
import com.bc.childgen.ChildGeneratorImpl;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SliceHandler that opens (and closes) a new HDFS file for each slice.
 * The files are named like the input file with the appendix .s<sliceindex> .
 *
 * @author Martin Boettcher
 */
public class SingleHdfsFileSliceHandler implements ChildGeneratorImpl.SliceHandler {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final String destination;
    private final FileSystem hdfs;

    private long numBytes = 0;
    long wt = 0;
    private long t0;
    private FSDataOutputStream outputStream;

    public SingleHdfsFileSliceHandler(FileSystem hdfs, String destination) {
        this.hdfs = hdfs;
        this.destination = destination;
    }

    public FormatPerformanceMetrics getFormatPerformanceMetrics() {
        return new FormatPerformanceMetrics(0, 0, numBytes, wt);
    }

    @Override
    public ImageOutputStream beginSlice(int sliceIndex, String productName, int firstLine, int lastLine) throws IOException {
        LOG.info(MessageFormat.format("Processing fragment {0} (line {1} ... {2})",
                                                 sliceIndex, firstLine, lastLine));
        Path destPath = new Path(destination, productName + ".s" + sliceIndex);
        t0 = System.nanoTime();
        outputStream = hdfs.create(destPath, true);
        MemoryCacheImageOutputStream imageOutputStream = new MemoryCacheImageOutputStream(outputStream);
        return imageOutputStream;
    }

    @Override
    public void endSlice(int sliceIndex, String productName, long bytesWritten) throws IOException {
        LOG.info(MessageFormat.format("Fragment {0} processed, bytes written: {1}",
                                                 sliceIndex, bytesWritten));
        wt += System.nanoTime() - t0;
        numBytes += bytesWritten;
        
        if (outputStream != null) {
            outputStream.close();
            outputStream = null;
        }
    }

    @Override
    public boolean handleError(int sliceIndex, IOException e) {
        LOG.log(Level.SEVERE, MessageFormat.format("Problem while processing fragment {0}",
                                                   sliceIndex), e);
        return true;
    }
}
