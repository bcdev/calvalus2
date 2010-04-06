package com.bc.calvados.hadoop.eodata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.dataio.envisat.DSD;
import org.esa.beam.dataio.envisat.ProductFile;
import org.esa.beam.dataio.envisat.RecordReader;

import javax.imageio.stream.FileCacheImageInputStream;
import javax.imageio.stream.FileImageOutputStream;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Locale;


public class N1ProductFileConverter implements ProductFileConverter {
    private static final String HDFS_PREFIX = "hdfs://";

    public void convertToMRFriendlyFormat(File inputFile, OutputStream outputStream) throws IOException {
        ProductFile productFile = ProductFile.open(inputFile);

        try {
            RecordReader[] recordReaders = getMdsRecordReaders(productFile);
            byte[][] recordBuffers = getMdsRecordBuffers(recordReaders);

            ImageInputStream inputStream = productFile.getDataInputStream();
            int headerSize = (int) recordReaders[0].getDSD().getDatasetOffset();

            byte[] headerBuffer = new byte[headerSize];
            inputStream.seek(0);
            inputStream.read(headerBuffer);
            outputStream.write(headerBuffer);

            int rasterHeight = productFile.getSceneRasterHeight();
            for (int y = 0; y < rasterHeight; y++) {
                for (int i = 0; i < recordReaders.length; i++) {
                    RecordReader recordReader = recordReaders[i];
                    DSD dsd = recordReader.getDSD();
                    long pos = dsd.getDatasetOffset() + (y * dsd.getRecordSize());

                    byte[] recordBuffer = recordBuffers[i];
                    inputStream.seek(pos);
                    inputStream.read(recordBuffer);
                    outputStream.write(recordBuffer);
                }
            }
        } finally {
            productFile.close();
        }
    }

    @Override
    public void convertFromMRFriendlyFormat(InputStream inputStream, File outputFile) throws IOException {

        FileCacheImageInputStream imageInputStream = new FileCacheImageInputStream(inputStream, new File("."));
        ProductFile productFile = ProductFile.open(imageInputStream);

        try {
            RecordReader[] recordReaders = getMdsRecordReaders(productFile);
            byte[][] recordBuffers = getMdsRecordBuffers(recordReaders);

            ImageOutputStream imageOutputStream = new FileImageOutputStream(outputFile);
            int headerSize = (int) recordReaders[0].getDSD().getDatasetOffset();

            byte[] headerBuffer = new byte[headerSize];
            imageInputStream.seek(0);
            imageInputStream.read(headerBuffer);
            imageOutputStream.write(headerBuffer);

            int rasterHeight = productFile.getSceneRasterHeight();
            for (int y = 0; y < rasterHeight; y++) {
                for (int i = 0; i < recordReaders.length; i++) {
                    RecordReader recordReader = recordReaders[i];
                    DSD dsd = recordReader.getDSD();
                    long pos = dsd.getDatasetOffset() + (y * dsd.getRecordSize());

                    byte[] recordBuffer = recordBuffers[i];
                    imageInputStream.read(recordBuffer);
                    imageOutputStream.seek(pos);
                    imageOutputStream.write(recordBuffer);
                }
            }
        } finally {
            productFile.close();
        }

    }

    private RecordReader[] getMdsRecordReaders(ProductFile productFile) throws IOException {
        String[] mdsNames = productFile.getValidDatasetNames('M');
        RecordReader[] recordReaders = new RecordReader[mdsNames.length];
        for (int i = 0; i < mdsNames.length; i++) {
            RecordReader recordReader = productFile.getRecordReader(mdsNames[i]);
            recordReaders[i] = recordReader;
        }
        return recordReaders;
    }

    private byte[][] getMdsRecordBuffers(RecordReader[] recordReaders) {
        byte[][] recordBuffers = new byte[recordReaders.length][];
        for (int i = 0; i < recordReaders.length; i++) {
            recordBuffers[i] = new byte[recordReaders[i].getDSD().getRecordSize()];
        }
        return recordBuffers;
    }

    /**
     * @param args
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException {
        Locale.setDefault(Locale.ENGLISH);

        boolean copy = false;
        boolean exceptions = false;
        boolean oneBlock = false;
        ArrayList<String> argList = new ArrayList<String>();
        for (String arg : args) {
            if (arg.startsWith("-")) {
                if (arg.equals("-c") || arg.equals("--copy")) {
                    copy = true;
                } else  if (arg.equals("-e") || arg.equals("--exceptions")) {
                        exceptions = true;
                } else  if (arg.equals("-o") || arg.equals("--oneblock")) {
                        oneBlock = true;
                } else {
                    System.out.println(MessageFormat.format("Error: Illegal option ''{0}''", arg));
                    System.exit(1);
                }
            } else {
                argList.add(arg);
            }
        }

        if (argList.size() != 2) {
            System.out.println("Usage: <input-file> <output-dir>");
            System.exit(1);
        }

        String inputFilePath = argList.get(0);
        String outputDirPath = argList.get(1);
        boolean fromHdfs = inputFilePath.startsWith(HDFS_PREFIX);
        boolean toHdfs = outputDirPath.startsWith(HDFS_PREFIX);

        try {
            long time = System.nanoTime();
            File inputFile = new File(inputFilePath);
            OutputStream outputStream = getOutputStream(inputFile, outputDirPath, toHdfs, oneBlock);
            if (copy) {
                System.out.println(MessageFormat.format("Copying ''{0}'' to ''{1}''...", inputFilePath, outputDirPath));
                copy(inputFile, outputStream);
            } else {
                System.out.println(MessageFormat.format("Converting ''{0}'' to ''{1}''...", inputFilePath, outputDirPath));
                convert(inputFile, outputStream);
            }
            double seconds = (System.nanoTime() - time) / 1.0E9;
            double megabytes = inputFile.length() / (1024.0 * 1024.0);
            System.out.println(MessageFormat.format("{0} MB written in {1} s at {2} MB/s", megabytes, seconds, megabytes / seconds));
        } catch (IOException e) {
            System.out.println(MessageFormat.format("I/O error: {0}", e.getMessage()));
            if (exceptions) {
                e.printStackTrace();
            }
            System.exit(2);
        } catch (Throwable e) {
            System.out.println(MessageFormat.format("Internal error: {0}", e.getMessage()));
            if (exceptions) {
                e.printStackTrace();
            }
            System.exit(3);
        }
    }

    private static OutputStream getOutputStream(File inputFile, String outputDirPath, boolean toHdfs, boolean oneBlock) throws IOException {
        OutputStream outputStream;
        if (!toHdfs) {
            File outputFile = new File(outputDirPath, inputFile.getName());
            outputStream = new BufferedOutputStream(new FileOutputStream(outputFile), 1024 * 1024);
        } else {
            int i = outputDirPath.indexOf("/", HDFS_PREFIX.length() + 1);
            String fsDefaultName = outputDirPath.substring(0, i);
            Path path = new Path(outputDirPath, inputFile.getName());
            Configuration configuration = new Configuration();
            configuration.set("fs.default.name", fsDefaultName);
            FileSystem fileSystem = FileSystem.get(configuration);
            if (oneBlock) {
                int bufferSize = fileSystem.getConf().getInt("io.file.buffer.size", 4096);
                int checksumSize = fileSystem.getConf().getInt("io.bytes.per.checksum", 512);
                short replication =fileSystem.getDefaultReplication();
                long fileSize = inputFile.length();
                // blocksize must be a multiple of checksum size
                long blockSize = ((fileSize / checksumSize) + 1) * checksumSize;
                outputStream = fileSystem.create(path, true, bufferSize, replication, blockSize);
            } else {
                outputStream = fileSystem.create(path, true);
            }
        }
        return outputStream;
    }

    private static void copy(File inputFile, OutputStream outputStream) throws IOException {
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(inputFile));
        byte[] bytes = new byte[1024 * 1024];
        int len;
        while ((len = inputStream.read(bytes)) > 0) {
            outputStream.write(bytes, 0, len);
        }
        inputStream.close();
        outputStream.close();
    }

    private static void convert(File inputFile, OutputStream outputStream) throws IOException {
        N1ProductFileConverter n1Converter = new N1ProductFileConverter();
        n1Converter.convertToMRFriendlyFormat(inputFile, outputStream);
        outputStream.close();
    }
}
