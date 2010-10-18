package com.bc.calvalus.experiments.transfer;

import com.bc.calvalus.experiments.format.CopyConverter;
import com.bc.calvalus.experiments.format.FileConverter;
import com.bc.calvalus.experiments.format.FormatPerformanceMetrics;
import com.bc.calvalus.experiments.format.N1ToLineInterleavedConverter;
import com.bc.calvalus.experiments.processing.Args;
import com.bc.calvalus.experiments.processing.LogFormatter;
import com.bc.childgen.ChildGenException;
import com.bc.childgen.ChildGeneratorFactory;
import com.bc.childgen.ChildGeneratorImpl;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.imageio.stream.FileImageInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hadoop client to transfer N1 files to (and from) HDFS in different formats.
 *
 * <pre>
 * Usage: TransferTool source destination format
 * Example: TransferTool ~/samples/MERIS-RR-2010-08 hdfs://cvmaster00:9000/data/transfer/MERIS-RR-2010-08 lineinterleaved
 * </pre>
 *
 * Supported formats: n1 (single block), lineinterleaved (default block size), sliced (into slices of block size)
 *
 * @author Martin Boettcher
 */
public class TransferTool extends Configured implements Tool {

    private static final String HDFS_PREFIX = "hdfs://";

    private static final int MER_RR_LINE_LENGTH = 1121;
    private static final int MER_RR_LINES_PER_SPLIT = 1121;
    private static final int MER_RR_BYTES_PER_SCAN = MER_RR_LINE_LENGTH * (15 * 2 + 2 + 1);
    private static final int MER_RR_BUFFER_SIZE = MER_RR_LINES_PER_SPLIT * MER_RR_BYTES_PER_SCAN;

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

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TransferTool(), args));
    }


    @Override
    public int run(String[] args) throws Exception {

        try {
            // parse command line arguments
            Args options = new Args(args);
            String source = options.getArgs()[0];
            String destination = options.getArgs()[1];
            String format = options.getArgs()[2];
            boolean isInputHdfs = source.startsWith(HDFS_PREFIX);
            boolean isOutputHdfs = destination.startsWith(HDFS_PREFIX);
            if (isInputHdfs == isOutputHdfs) {
                throw new IllegalArgumentException("one of source and destination shall be local and one HDFS respectively: " + source + " " + destination);
            }

            // transfer to HDFS ...
            LOG.info("starting " + format + " transfer from " + source + " to " + destination);
            FormatPerformanceMetrics metrics = new FormatPerformanceMetrics(0, 0, 0, 0);
            if (isOutputHdfs) {
                FileSystem hdfs = new Path(destination).getFileSystem(getConf());

                File sourceDir = new File(source);
                File[] sourceFiles = sourceDir.listFiles();
                if (sourceFiles == null) {
                    throw new IllegalArgumentException("cannot read source dir " + source);
                }

                for (File sourceFile : sourceFiles) {
                    // distinguish formats
                    if ("n1".equals(format)) {
                        metrics.add(toSingleBlockN1(hdfs, sourceFile, destination));
                    } else if ("lineinterleaved".equals(format)) {
                        metrics.add(toInterleaved(hdfs, sourceFile, destination));
                    } else if ("sliced".equals(format)) {
                        metrics.add(toSlices(hdfs, sourceFile, destination));
                    } else {
                        throw new IllegalArgumentException("one of lineinterleaved, sliced, n1 expected for format, found " + format);
                    }
                }
            }
            // transfer from HDFS ...
            else
            {
                Path inputDir = new Path(source);
                FileSystem hdfs = inputDir.getFileSystem(getConf());
                FileStatus[] inputs = hdfs.listStatus(inputDir);

                File destDir = new File(destination);
                destDir.mkdirs();

                for (FileStatus input : inputs) {
                    Path inputPath = input.getPath();
                    // distinguish formats
                    if ("n1".equals(format)) {
                        metrics.add(copyFrom(hdfs, inputPath, destDir));
                    } else if ("lineinterleaved".equals(format)) {
                        metrics.add(fromInterleaved(hdfs, inputPath, destDir));
                    } else if ("sliced".equals(format)) {
                        metrics.add(copyFrom(hdfs, inputPath, destDir));
                    } else {
                        throw new IllegalArgumentException("one of lineinterleaved, sliced, n1 expected for format, found " + format);
                    }
                }
            }
            LOG.info("stopping " + format + " transfer from " + source + " to " + destination);
            LOG.info("metrics: " +  metrics);

            return 0;

        } catch (IllegalArgumentException ex) {

            System.err.println("failed: " + ex.getMessage());
            return 1;

        } catch (ArrayIndexOutOfBoundsException ex) {

            System.err.println("usage: TransferTool <source> <destination> <format>");
            System.err.println("example: TransferTool ~/samples/MERIS-RR-2010-08 hdfs://cvmaster00:9000/data/transfer/MERIS-RR-2010-08 lineinterleaved");
            return 1;
        }
    }

    /**
     * Writes the complete product to a large block in HDFS to ensure that it is not distributed.
     *
     * @param hdfs        the target file system
     * @param inputFile   the local input file
     * @param destination the target directory URL hdfs://...
     * @throws IOException if reading or writing fails
     */
    private FormatPerformanceMetrics toSingleBlockN1(FileSystem hdfs, File inputFile, String destination) throws IOException {

        OutputStream out = null;
        try {
            out = createSingleBlockOutputStream(hdfs, inputFile, destination);
            FileConverter converter = new CopyConverter();
            return converter.convertTo(inputFile, out);
        } finally {
            if (out != null) out.close();
        }
    }

    private OutputStream createSingleBlockOutputStream(FileSystem hdfs, File inputFile, String destination) throws IOException {

        // calculate block size to cover complete N1
        // blocksize must be a multiple of checksum size
        int bufferSize = hdfs.getConf().getInt("io.file.buffer.size", 4096);
        int checksumSize = hdfs.getConf().getInt("io.bytes.per.checksum", 512);
        short replication = hdfs.getDefaultReplication();
        long fileSize = inputFile.length();
        long blockSize = ((fileSize / checksumSize) + 1) * checksumSize;

        // construct HDFS output stream
        Path destPath = new Path(destination, inputFile.getName());
        return hdfs.create(destPath, true, bufferSize, replication, blockSize);
    }

    /**
     * Writes the product to line interleaved format with the default block size of the target file system
     *
     * @param hdfs        the target file system
     * @param inputFile   the local input file
     * @param destination the target directory URL hdfs://...
     * @throws IOException if reading or writing fails
     */
    private FormatPerformanceMetrics toInterleaved(FileSystem hdfs, File inputFile, String destination) throws IOException {

        OutputStream out = null;
        try {
            Path destPath = new Path(destination, inputFile.getName() + ".li");
            out = hdfs.create(destPath, true);
            FileConverter converter = new N1ToLineInterleavedConverter();
            return converter.convertTo(inputFile, out);
        } finally {
            if (out != null) out.close();
        }
    }

    /**
     * Writes source data to slices sized smaller than or equal sized to block size.
     *
     * @param hdfs        the target file system
     * @param inputFile   the local input file
     * @param destination the target directory URL hdfs://...
     * @throws IOException if reading or writing fails
     */
    private FormatPerformanceMetrics toSlices(FileSystem hdfs, File inputFile, String destination) throws IOException {
        long blockSize = hdfs.getDefaultBlockSize();
        FileImageInputStream in = new FileImageInputStream(inputFile);
        ChildGeneratorImpl childGenerator = null;
        try {
            childGenerator = ChildGeneratorFactory.createChildGenerator(inputFile.getName());
        } catch (ChildGenException ex) {
            throw new IOException("child generation failed: " + ex.getMessage(), ex);
        }
        // compute number of lines from block size, slice header size, and record size
        int lines = MER_RR_LINES_PER_SPLIT;  // TODO compute
        childGenerator.slice(in,
                             lines,
                             new HdfsSliceHandler(hdfs, destination));
        return new FormatPerformanceMetrics(-1, -1, -1, -1);  // TODO measure
    }


    /**
     * Copies a file from HDFS to the local file system
     *
     * @param hdfs        the source file system
     * @param inputPath   the HDFS source Path of the input file
     * @param destDir     the target directory in the local file system
     * @throws IOException if reading or writing fails
     */
    private FormatPerformanceMetrics copyFrom(FileSystem hdfs, Path inputPath, File destDir) throws IOException {

        InputStream in = null;
        try {
            in = hdfs.open(inputPath);
            FileConverter converter = new CopyConverter();
            return converter.convertFrom(in, new File(destDir, inputPath.getName()));
        } finally {
            if (in != null) in.close();
        }
    }

    /**
     * Converts the product stored in HDFS in line-interleaved format back to an N1 in the local file system
     *
     * @param hdfs        the source file system
     * @param inputPath   the HDFS source Path of the input file
     * @param destDir     the target directory in the local file system
     * @throws IOException if reading or writing fails
     */
    private FormatPerformanceMetrics fromInterleaved(FileSystem hdfs, Path inputPath, File destDir) throws IOException {

        InputStream in = null;
        try {
            in = hdfs.open(inputPath);
            FileConverter converter = new N1ToLineInterleavedConverter();
            return converter.convertFrom(in, new File(destDir, trim(inputPath.getName(), ".li")));
        } finally {
            if (in != null) in.close();
        }
    }

    /**
     * @return  string shortened by tail if string ends with tail
     */
    private static String trim(String s, String tail) {
        if (s.endsWith(tail)) {
            return s.substring(0, s.length() - tail.length());
        } else {
            return s;
        }
    }
}
