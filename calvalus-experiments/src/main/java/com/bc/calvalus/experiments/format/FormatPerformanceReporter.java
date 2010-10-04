package com.bc.calvalus.experiments.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.Locale;


// todo - Also measure N1ToSequenceFile (nf - 04.10.2010)
// todo - Also measure CopyConverter (nf - 04.10.2010)
// todo - For each format measure read and write (nf - 04.10.2010) 
// todo - Output CSV format (nf - 04.10.2010)

/**
 * A tool used to perform some performance tests on several data storage formats.
 *
 * <pre>
 *    Usage:
 *        todo!
 * </pre>
 * @author Norman Fomferra
 * @author Marco Zuehlke
 * @since 0.1
 */
public class FormatPerformanceReporter {

    private static final String HDFS_PREFIX = "hdfs://";

    // todo - read from properties file (nf - 04.10.2010)
    private static final String productDir = "/home/marcoz/EOData/Meris/L1b/";
    private static final String localTmpDir = "/tmp/calvalus";
    private static final String localStorageDir = "/tmp/calvalus/Repo/";
    private static final String networkStorageDir = "/mnt/BCserver2/temp/calvadostest/";
    private static final String hdfsStorageDir = "hdfs://hadoop000.bc.local:9000/";
    private final String[] productNames;

    private enum MODE {
        COPY,
        CONVERT,
        RECONVERT
    }

    public static void main(String[] args) throws IOException {
        Locale.setDefault(Locale.ENGLISH);

        String[] productNames = getProductNames(productDir);
        new FormatPerformanceReporter(productNames).run();
    }

    private static String[] getProductNames(String productDir) {
        return new String[]{
//                "MER_RR__1PP01R20030401_095958_000001972015_00108_05673_0001.N1",  //small (40MB)
                "MER_RR__1PNPDK20040607_094206_000022072027_00294_11871_0978.N1",    // full orbit (400MB)
                "MER_RR__1PNPDK20070803_093844_000021392060_00251_28361_1490.N1"     // full orbit (400MB)
        };
    }

    FormatPerformanceReporter(String[] productNames) {
        this.productNames = productNames;
    }

    private void run() throws IOException {
        doProcess("Copy: local to local", productDir, localStorageDir, MODE.COPY);
        doProcess("Copy: local to network", productDir, networkStorageDir, MODE.COPY);
        doProcess("Copy: local to hdfs", productDir, hdfsStorageDir, MODE.COPY);
        doProcess("Copy: network to local", networkStorageDir, localTmpDir, MODE.COPY);
        doProcess("Copy: hdfs to local", hdfsStorageDir, localTmpDir, MODE.COPY);

        doProcess("Convert: local to local", productDir, localStorageDir, MODE.CONVERT);
        doProcess("Convert: local to network", productDir, networkStorageDir, MODE.CONVERT);
        doProcess("Convert: local to hdfs", productDir, hdfsStorageDir, MODE.CONVERT);

        doProcess("ReConvert: local to local", localStorageDir, localTmpDir, MODE.RECONVERT);
        doProcess("ReConvert: network to local", networkStorageDir, localTmpDir, MODE.RECONVERT);
        doProcess("ReConvert: hdfs to local", hdfsStorageDir, localTmpDir, MODE.RECONVERT);
    }

    private void doProcess(String msg, String srcDir, String destDir, MODE mode) throws IOException {
        ensureDirExist(srcDir);
        ensureDirExist(destDir);
        FileConverter fileConverter = new N1ToLineInterleavedConverter();

        System.out.println(MessageFormat.format("{0} - ''{1}'' to ''{2}''...", msg, srcDir, destDir));
        for (String productName : productNames) {
            long time = System.nanoTime();
            long length = 0;
            try {
                switch (mode) {
                    case COPY: {
                        final InputStream inputStream = getInputStream(srcDir, productName);
                        final OutputStream outputStream = getOutputStream(destDir, productName);
                        length = CopyConverter.copy(inputStream, outputStream);
                        inputStream.close();
                        outputStream.close();
                        break;
                    }
                    case CONVERT: {
                        final File inputFile = new File(srcDir, productName);
                        length = inputFile.length();
                        final OutputStream outputStream = getOutputStream(destDir, productName);
                        fileConverter.convertTo(inputFile, outputStream);
                        outputStream.close();
                        break;
                    }
                    case RECONVERT: {
                        final InputStream inputStream = getInputStream(srcDir, productName);
                        final File outputFile = new File(destDir, productName);
                        fileConverter.convertFrom(inputStream, outputFile);
                        inputStream.close();
                        length = outputFile.length();
                        break;
                    }
                }
                double seconds = (System.nanoTime() - time) / 1.0E9;
                double megabytes = length / (1024.0 * 1024.0);
                System.out.println(MessageFormat.format("{0} MB written in {1} s at {2} MB/s", megabytes, seconds, megabytes / seconds));
            } catch (IOException e) {
                System.out.println(MessageFormat.format("I/O error: {0}", e.getMessage()));
                e.printStackTrace();
                System.exit(2);
            } catch (Throwable e) {
                System.out.println(MessageFormat.format("Internal error: {0}", e.getMessage()));
                e.printStackTrace();
                System.exit(3);
            }
        }
    }

    private void ensureDirExist(String dir) {
        boolean isHdfs = dir.startsWith(HDFS_PREFIX);
        if (!isHdfs) {
            final File file = new File(dir);
            file.mkdirs();
        }
    }

    private InputStream getInputStream(String srcDir, String name) throws IOException {
        boolean isHdfs = srcDir.startsWith(HDFS_PREFIX);
        InputStream inStream;
        if (isHdfs) {
            int i = srcDir.indexOf("/", HDFS_PREFIX.length() + 1);
            String fsDefaultName = srcDir.substring(0, i);
            Path path = new Path(srcDir, name);
            Configuration configuration = new Configuration();
            configuration.set("fs.default.name", fsDefaultName);
            FileSystem fileSystem = FileSystem.get(configuration);
            inStream = fileSystem.open(path);
        } else {
            File inputFile = new File(srcDir, name);
            inStream = new BufferedInputStream(new FileInputStream(inputFile), 1024 * 1024);
        }
        return inStream;
    }

    private static OutputStream getOutputStream(String outputDirPath, String inputFileName) throws IOException {
        boolean toHdfs = outputDirPath.startsWith(HDFS_PREFIX);
        OutputStream outputStream;
        if (toHdfs) {
            int i = outputDirPath.indexOf("/", HDFS_PREFIX.length() + 1);
            String fsDefaultName = outputDirPath.substring(0, i);
            Path path = new Path(outputDirPath, inputFileName);
            Configuration configuration = new Configuration();
            configuration.set("fs.default.name", fsDefaultName);
            FileSystem fileSystem = FileSystem.get(configuration);
            outputStream = fileSystem.create(path, true);
        } else {
            File outputFile = new File(outputDirPath, inputFileName);
//            outputFile.createNewFile();
            outputStream = new BufferedOutputStream(new FileOutputStream(outputFile), 1024 * 1024);
        }
        return outputStream;
    }
}
