package com.bc.calvados.hadoop.eodata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.dataio.envisat.DSD;
import org.esa.beam.dataio.envisat.ProductFile;
import org.esa.beam.dataio.envisat.RecordReader;

import javax.imageio.stream.ImageInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Locale;


public class N1ProductFileConverter implements ProductFileConverter {

    public void convert(File inputFile, OutputStream outputStream) throws IOException {
        ProductFile productFile = ProductFile.open(inputFile);

        try {
            String[] mdsNames = productFile.getValidDatasetNames('M');
            RecordReader[] recordReaders = new RecordReader[mdsNames.length];
            byte[][] recordBuffers = new byte[recordReaders.length][];
            for (int i = 0; i < mdsNames.length; i++) {
                RecordReader recordReader = productFile.getRecordReader(mdsNames[i]);
                recordReaders[i] = recordReader;
                recordBuffers[i] = new byte[recordReader.getDSD().getRecordSize()];
            }

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

    public static void main(String[] args) throws FileNotFoundException {
        Locale.setDefault(Locale.ENGLISH);

        if (args.length != 2) {
            System.out.println("Usage: <input-file> <output-file>");
            System.exit(1);
        }

        String inputPathName = args[0];
        String outputPathName = args[1];
        boolean distributed = outputPathName.startsWith("hdfs://");


        try {
            long time = System.nanoTime();

            File inputFile = new File(inputPathName);

            OutputStream outputStream;
            System.out.printf("Converting '%s' to '%s'...%n", inputPathName, outputPathName);
            if (!distributed) {
                File outputFile = new File(outputPathName);
                outputStream = new BufferedOutputStream(new FileOutputStream(outputFile), 1024 * 1024);
            } else {
                int i = outputPathName.indexOf("/", "hdfs://".length() + 1);
                String fsDefaultName = outputPathName.substring(0, i);
                outputPathName = outputPathName.substring(i);

                // System.out.println("fsDefaultName = " + fsDefaultName);
                // System.out.println("outputPathName = " + outputPathName);

                Path path = new Path(args[1]);
                Configuration configuration = new Configuration();
                configuration.set("fs.default.name", fsDefaultName);
                FileSystem fileSystem = FileSystem.get(configuration);
                outputStream = fileSystem.create(path, true);
            }

            ProductFileConverter n1Converter = new N1ProductFileConverter();
            n1Converter.convert(inputFile, outputStream);
            outputStream.close();

            time = System.nanoTime() - time;
            System.out.printf("Converted in %f ms at %f bytes/s%n", time / 1.0E6, (inputFile.length() * 1.0E9) / time);
        } catch (IOException e) {
            System.out.printf("I/O error: %s%n", e.getMessage());
            System.exit(2);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(3);
        }
    }
}
