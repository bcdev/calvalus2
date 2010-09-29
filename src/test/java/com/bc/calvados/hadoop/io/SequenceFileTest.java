package com.bc.calvados.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SequenceFileTest {
    @Test
    public void testIt() throws IOException {
        File baseDir = new File("/fs1/temp/forNorman");
        List<File> files = getFileList(baseDir);

        testIO(baseDir, files, SequenceFile.CompressionType.NONE);
        testIO(baseDir, files, SequenceFile.CompressionType.RECORD);
        testIO(baseDir, files, SequenceFile.CompressionType.BLOCK);
    }

    // this both a test for the SequenceFile API and also for its performance
    //
    private void testIO(File baseDir, List<File> files, SequenceFile.CompressionType compressionType) throws IOException {
        long t0, t1;

        Path outputFile = new Path(baseDir.getPath(), compressionType+".seq");

        System.out.println("baseDir = " + baseDir);
        System.out.println("outputFile = " + outputFile);
        System.out.println("compressionType = " + compressionType);

        t0 = System.currentTimeMillis();
        SequenceFile.Metadata metadata = new SequenceFile.Metadata();
        metadata.set(new Text("numRecords"), new Text("" + files.size()));

        Configuration configuration = new Configuration();
        SequenceFile.Writer writer = SequenceFile.createWriter(LocalFileSystem.get(configuration),
                                                               configuration,
                                                               outputFile,
                                                               Text.class,
                                                               BytesWritable.class,
                                                               compressionType,
                                                               new DefaultCodec(),
                                                               null, metadata);
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        for (File file : files) {
            key.set(file.getPath());
            loadFile(file, value);
            writer.append(key, value);
        }
        writer.close();
        t1 = System.currentTimeMillis();
        System.out.println("Write: " + (t1 - t0) + " ms");

        t0 = System.currentTimeMillis();
        SequenceFile.Reader reader = new SequenceFile.Reader(LocalFileSystem.get(configuration),
                                                             outputFile,
                                                             configuration);
        key = new Text();
        value = new BytesWritable();
        int index = 0;
        while (true) {
            boolean more = reader.next(key, value);
            if (!more) {
                break;
            }
            assertEquals(files.get(index).getPath(), key.toString());
            index++;
        }
        reader.close();
        t1 = System.currentTimeMillis();
        System.out.println("Read: " + (t1 - t0) + " ms");
    }

    private void loadFile(File file, BytesWritable value) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        try {
            value.setSize((int) file.length());
            inputStream.read(value.getBytes(), 0, (int) file.length());
        } finally {
            inputStream.close();
        }
    }

    private List<File> getFileList(File dir) {
        ArrayList<File> files = new ArrayList<File>();
        collectFiles(dir, files);
        return files;
    }

    private void collectFiles(File dir, List<File> list) {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isFile()) {
                list.add(file);
            }
        }
        for (File file : files) {
            if (file.isDirectory()) {
                collectFiles(file, list);
            }
        }
    }
}
