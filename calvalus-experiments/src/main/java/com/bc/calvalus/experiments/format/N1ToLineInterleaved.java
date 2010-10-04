package com.bc.calvalus.experiments.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Locale;

/**
 * A tool that converts Envisat N1 files from their band-interleaved format to line-interleaved.
 *
 * <pre>
 *  Usage:
 *     N1ToLineInterleaved <input-file> <output-dir>
 * </pre>
 *
 * @author Norman
 * @since 0.1
 */
public class N1ToLineInterleaved {
    private static final String HDFS_PREFIX = "hdfs://";

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
                } else if (arg.equals("-e") || arg.equals("--exceptions")) {
                    exceptions = true;
                } else if (arg.equals("-o") || arg.equals("--oneblock")) {
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
                short replication = fileSystem.getDefaultReplication();
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
        FileConverter fileConverter = new N1ToLineInterleavedConverter();
        fileConverter.convertTo(inputFile, outputStream);
        outputStream.close();
    }
}
