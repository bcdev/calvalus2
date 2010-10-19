package com.bc.calvalus.experiments.format.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;

/**
 * Tool that writes the contents of directories into a sequence file and outputs some I/O statistics.
 * The generated file can be read using {@link ReadSequenceFile}.
 * <pre>
 * Usage:
 *    WriteSequenceFile [-c] &lt;seq-file&gt; &lt;dir1&gt; &lt;dir2&gt; ...
 * </pre>

 * @author Norman
 * @since 0.1
 */
public class WriteSequenceFile {
    public static void main(String[] args) throws IOException {
        ArrayList<String> argList = new ArrayList<String>();
        boolean compressed = false;
        for (String arg : args) {
            if (arg.equals("-c")) {
                compressed = true;
            } else {
                argList.add(arg);
            }
        }
        ArrayList<File> fileList = new ArrayList<File>();
        for (int i = 1; i < argList.size(); i++) {
            File dir = new File(argList.get(1));
            collectFiles(dir, fileList);
        }
        writeFiles(fileList, argList.get(0), compressed);
    }

    private static void writeFiles(ArrayList<File> fileList, String outputFile, boolean compressed) throws IOException {
        Path path = new Path(outputFile);
        System.out.println(MessageFormat.format("Writing {0} file {1}...",
                                                compressed ? "compressed" : "non-compressed",
                                                path));
        Configuration configuration = new Configuration();
        FileSystem fileSystem = LocalFileSystem.get(configuration);
        SequenceFile.Metadata metadata = new SequenceFile.Metadata();
        metadata.set(new Text("outputFile"), new Text(outputFile));
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem,
                                                               configuration,
                                                               path,
                                                               Text.class,
                                                               ByteArrayFactory.getType(compressed),
                                                               SequenceFile.CompressionType.RECORD,
                                                               new DefaultCodec(),
                                                               null,
                                                               metadata);

        long bytesTotal = 0;
        long timeTotal = 0;
        for (File file : fileList) {
            byte[] data = readFile(file);
            System.out.println(MessageFormat.format("Writing file {0}, {1} bytes", file, data.length, compressed ? "compressed" : "raw"));
            long t0 = System.nanoTime();
            writer.append(new Text(file.getPath()),
                          ByteArrayFactory.createByteArray(compressed, data));
            long t1 = System.nanoTime();
            timeTotal += (t1 - t0);
            bytesTotal += data.length;
        }
        writer.close();

        System.out.println(MessageFormat.format("{0} files read, {1} bytes total.", fileList.size(), bytesTotal));

        FileStatus[] stati = fileSystem.listStatus(path);
        for (FileStatus status : stati) {
            System.out.println(MessageFormat.format("{0} written, size is {1} bytes, {2} ms for writing.", status.getPath(), status.getLen(), timeTotal / 1E9));
        }
    }

    private static byte[] readFile(File file) throws IOException {
        byte[] data = new byte[(int) file.length()];
        FileInputStream inputStream = new FileInputStream(file);
        inputStream.read(data);
        inputStream.close();
        return data;
    }

    private static void collectFiles(File dir, ArrayList<File> fileList) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    fileList.add(file);
                }
            }
            for (File file : files) {
                if (file.isDirectory()) {
                    collectFiles(file, fileList);
                }
            }
        }
    }
}
