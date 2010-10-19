package com.bc.calvalus.experiments.format.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;

/**
 * Tool that reads a sequence file generated using {@link WriteSequenceFile} and outputs some I/O statistics.
 * <pre>
 * Usage:
 *    ReadSequenceFile [-c] &lt;seq-file&gt;
 * </pre>
 * @author Norman
 * @since 0.1
 */
public class ReadSequenceFile {
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
        readFile(argList.get(0), compressed);
    }

    private static void readFile(String file, boolean compressed) throws IOException {
        Path path = new Path(file);
        System.out.println(MessageFormat.format("Reading {0} file {1}...", 
                                                compressed ? "compressed" : "non-compressed",
                                                path));
        Configuration configuration = new Configuration();
        FileSystem fileSystem = LocalFileSystem.get(configuration);
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, configuration);
        Text key = new Text();
        ByteArray value = ByteArrayFactory.createByteArray(compressed);
        long timeTotal = 0;
        long bytesTotal = 0;
        while (true) {
            long t0 = System.nanoTime();
            boolean b = reader.next(key, value);
            long t1 = System.nanoTime();
            if (!b) {
                break;
            }
            System.out.println("" + key + ": " + value.getLength() + " bytes");
            timeTotal += (t1 - t0);
            bytesTotal += value.getLength();
        }
        System.out.println(MessageFormat.format("{0} bytes read in {1} ms", bytesTotal, timeTotal / 1E9 ));
    }

}
