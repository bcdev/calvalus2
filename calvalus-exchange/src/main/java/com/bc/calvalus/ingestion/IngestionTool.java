package com.bc.calvalus.ingestion;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.OutputStream;
import java.text.MessageFormat;


/**
 * A tool to archive all MER_RR__1P products found in a given directory into a computed directory in the HDFS archive.
 * <pre>
 * Usage:
 *    hadoop --config ${configDir} jar ${jobJar} com.bc.calvalus.ingestion.IngestionTool ${sourceDir}
 * </pre>
 */
public class IngestionTool extends Configured implements Tool {
    private static final String PRODUCT_TYPE = "MER_RR__1P";

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new IngestionTool(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println(MessageFormat.format("Usage: {0} <path>", IngestionTool.class.getSimpleName()));
            return 1;
        }

        File sourceDir = new File(args[0]);
        File[] sourceFiles = sourceDir.listFiles(new ProductFilenameFilter());
        if (sourceFiles == null) {
            System.err.println(MessageFormat.format("No {0} files found in {1}", PRODUCT_TYPE, sourceDir));
            return 2;
        }

        FileSystem hdfs = FileSystem.get(getConf());

        // calculate block size to cover complete N1
        // blocksize must be a multiple of checksum size
        //
        final int bufferSize = hdfs.getConf().getInt("io.file.buffer.size", 4096);
        final int checksumSize = hdfs.getConf().getInt("io.bytes.per.checksum", 512);
        final short replication = hdfs.getDefaultReplication();

        for (File sourceFile : sourceFiles) {
            String archivePath = getArchivePath(sourceFile.getName());
            System.out.println(MessageFormat.format("Archiving {0} in {1}", sourceFile, archivePath));

            long fileSize = sourceFile.length();
            long blockSize = ((fileSize / checksumSize) + 1) * checksumSize;

            // construct HDFS output stream
            //
            Path destPath = new Path(archivePath, sourceFile.getName());
            OutputStream out = hdfs.create(destPath, true, bufferSize, replication, blockSize);
            IOUtils.copyBytes(new FileInputStream(sourceFile), out, getConf(), true);
        }

        return 0;
    }


    /**
     * Implements the archiving "rule"
     * @param fileName a file name
     * @return   an archive path
     */
    static String getArchivePath(String fileName) {
        String subPath = getDatePath(fileName);
        return "/calvalus/eodata/" + PRODUCT_TYPE + "/r03/" + subPath;
    }

    static String getDatePath(String fileName) {
        return String.format("%s/%s/%s",
                             fileName.substring(14, 18),
                             fileName.substring(18, 20),
                             fileName.substring(20, 22));  
    }

    static class ProductFilenameFilter implements FilenameFilter {
        @Override
        public boolean accept(File file, String s) {
            return s.startsWith(PRODUCT_TYPE) && s.endsWith(".N1");
        }
    }
}
