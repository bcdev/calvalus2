package com.bc.calvalus.experiments.format;

import com.bc.calvalus.hadoop.io.ByteArrayWritable;
import com.bc.childgen.ChildGeneratorFactory;
import com.bc.childgen.ChildGeneratorImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Progressable;
import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.dataio.envisat.ProductFile;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Product;

import javax.imageio.stream.FileCacheImageInputStream;
import javax.imageio.stream.FileCacheImageOutputStream;
import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.awt.Rectangle;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

// todo - make this a file converter and test it by FormatPerformanceReporter (nf - 04.10.2010)

/**
 * A tool used to convert Envisat MERIS RR N1 files to Hadoop Sequence Files.
 * The N1 file ist first split into "children" using the <a href="https://github.com/bcdev/eo-child-gen">EO-Childgen</a> tool.
 *
 * <pre>
 * Usage:
 *    N1ToSequenceFile <mer-rr-n1> <output-dir>
 * </pre>
 *
 * @author Norman Fomferra
 * @since 0.1
 */
public class N1ToSequenceFile {

    private static final String VERSION = "0.1-20101001-01";

    /**
     * Estimation of split size for MERIS RR:
     * bytesPerScan = 1121 * (15 * 2 + 2 + 1) = 36993 B
     * --> 1121 * bytesPerScan = 40 MB
     */
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
        handler.setFormatter(new MyLogFormatter());
        LOG.addHandler(handler);
        LOG.setLevel(Level.ALL);
    }

    public static void main(String[] args) {
        if (args.length == 1) {
            String inputFile = args[0];
            new N1ToSequenceFile().readSF(inputFile);
        } else if (args.length == 2) {
            String inputFile = args[0];
            String outputDir = args[1];
            new N1ToSequenceFile().convertN1ToSF(inputFile, outputDir);
        } else {
            System.out.println("Usage: " + N1ToSequenceFile.class.getName() + " <mer-rr-n1> <output-dir>");
            System.exit(1);
        }

    }

    private void readSF(String path) {
        try {
            Path inputFile = new Path(path);
            Configuration configuration = new Configuration();
            FileSystem fs = LocalFileSystem.get(configuration);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, inputFile, configuration);

            IntWritable key = new IntWritable();
            ByteArrayWritable value = new ByteArrayWritable();
            while (true) {
                boolean b = reader.next(key, value);
                if (!b) {
                    break;
                }
                byte[] array = value.getArray();
                FileCacheImageInputStream iis = new FileCacheImageInputStream(new ByteArrayInputStream(array), null);
                EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
                ProductReader productReader = plugIn.createReaderInstance();
                ProductFile productFile = ProductFile.open(iis);
                Product product = productReader.readProductNodes(productFile, null);
                String name = product.getName();
                int rasterWidth = product.getSceneRasterWidth();
                int rasterHeight = product.getSceneRasterHeight();
                int numBands = product.getNumBands();

                System.out.printf("%s: %d x %d x %d\n", name, rasterWidth, rasterHeight, numBands);

                Raster data = product.getBand("radiance_13").getGeophysicalImage().getData(new Rectangle(100, 100, 10, 1));
                for (int i = 0; i < 10; i++) {
                    float aFloat = data.getSampleFloat(100, 100 + i, 0);
                    System.out.println("s[" + i + "] = " + aFloat);
                }

                ProductIO.writeProduct(product, name + ".dim", "BEAM-DIMAP");
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private void convertN1ToSF(String inputFileName, String outputDir) {

        ChildGeneratorImpl childGenerator;
        try {
            File inputFile = new File(inputFileName);
            FileImageInputStream iis = new FileImageInputStream(inputFile);

            String[][] metadataKeyValues = new String[][]{
                    {"originalFile.name", inputFile.getName()},
                    {"originalFile.length", "" + inputFile.length()},
                    {"originalFile.lastModified", "" + inputFile.lastModified()},
                    {"converter.name", N1ToSequenceFile.class.getName()},
                    {"converter.version", VERSION},
                    {"converter.timestamp", new Date().toString()},
            };

            SequenceFile.Metadata metadata = new SequenceFile.Metadata();
            for (String[] metadataKeyValue : metadataKeyValues) {
                metadata.set(new Text(metadataKeyValue[0]), new Text(metadataKeyValue[1]));
            }


            Path outputFile = new Path(outputDir, inputFile.getName() + ".seq");
            Configuration configuration = new Configuration();
            FileSystem fs = LocalFileSystem.get(configuration);
            fs.delete(outputFile, false);
            SequenceFile.Writer sequenceFileWriter = SequenceFile.createWriter(fs,
                                                                               configuration,
                                                                               outputFile,
                                                                               IntWritable.class,
                                                                               ByteArrayWritable.class,
                                                                               SequenceFile.CompressionType.NONE,
                                                                               null, // new DefaultCodec(),
                                                                               new MyProgressable(),
                                                                               metadata);

            try {
                childGenerator = ChildGeneratorFactory.createChildGenerator(inputFileName);
                childGenerator.fragment(iis,
                                        MER_RR_LINES_PER_SPLIT,
                                        new MyFragmentHandler(sequenceFileWriter));
            } catch (IOException e) {
                fs.delete(outputFile, false);
                throw e;
            } finally {
                IOUtils.closeStream(sequenceFileWriter);
                iis.close();
            }
        } catch (Throwable e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private class MyFragmentHandler implements ChildGeneratorImpl.FragmentHandler {
        private final SequenceFile.Writer sequenceFileWriter;
        private final MyByteArrayOutputStream arrayOutputStream;

        public MyFragmentHandler(SequenceFile.Writer sequenceFileWriter) {
            this.sequenceFileWriter = sequenceFileWriter;
            this.arrayOutputStream = new MyByteArrayOutputStream(MER_RR_BUFFER_SIZE);
        }

        @Override
        public ImageOutputStream beginFragment(int fragmentIndex, String productName, int firstLine, int lastLine) throws IOException {
            LOG.log(Level.INFO, MessageFormat.format("Processing fragment {0} (line {1} ... {2})",
                                                     fragmentIndex, firstLine, lastLine));
            arrayOutputStream.reset();
            return new FileCacheImageOutputStream(arrayOutputStream, null);
        }

        @Override
        public void endFragment(int fragmentIndex, String productName, long bytesWritten) throws IOException {
            sequenceFileWriter.append(new IntWritable(fragmentIndex),
                                      new ByteArrayWritable(arrayOutputStream.getInternalBuffer()));
            LOG.log(Level.INFO, MessageFormat.format("Fragment {0} processed, bytes written: {1}",
                                                     fragmentIndex, bytesWritten));
        }

        @Override
        public void handleError(int fragmentIndex, IOException e) {
            LOG.log(Level.SEVERE, MessageFormat.format("Problem while processing fragment {0}",
                                                       fragmentIndex), e);
        }

    }

    private class MyByteArrayOutputStream extends ByteArrayOutputStream {
        public MyByteArrayOutputStream(int capacity) {
            super(capacity);
        }

        public byte[] getInternalBuffer() {
            return buf;
        }
    }

    private static class MyProgressable implements Progressable {
        @Override
        public void progress() {

        }
    }

    private static class MyLogFormatter extends Formatter {
        @Override
        public String format(LogRecord logRecord) {
            StringBuilder sb = new StringBuilder(MessageFormat.format("{0}: {1} - {2}\n",
                                                                      logRecord.getLevel(),
                                                                      new Date(logRecord.getMillis()),
                                                                      logRecord.getMessage()));
            Throwable throwable = logRecord.getThrown();
            if (throwable != null) {
                StringWriter writer = new StringWriter();
                throwable.printStackTrace(new PrintWriter(writer));
                sb.append(writer.toString());
                sb.append("\n");
            }
            return sb.toString();
        }
    }
}
