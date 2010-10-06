package com.bc.calvalus.experiments.format;

import org.esa.beam.dataio.envisat.DSD;
import org.esa.beam.dataio.envisat.ProductFile;
import org.esa.beam.dataio.envisat.RecordReader;

import javax.imageio.stream.FileCacheImageInputStream;
import javax.imageio.stream.FileImageOutputStream;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Converts Envisat N1 files from their band-interleaved format to line-interleaved.
 * @author Norman Fomferra
 * @since 0.1
 */
public class N1ToLineInterleavedConverter implements FileConverter {

    public FormatPerformanceMetrics convertTo(File inputFile, OutputStream outputStream) throws IOException {

        ProductFile productFile = ProductFile.open(inputFile);

        try {
            long numBytes = 0;
            long t0;
            long rt = 0;
            long wt = 0;

            RecordReader[] recordReaders = getMdsRecordReaders(productFile);
            byte[][] recordBuffers = getMdsRecordBuffers(recordReaders);

            ImageInputStream inputStream = productFile.getDataInputStream();
            int headerSize = (int) recordReaders[0].getDSD().getDatasetOffset();

            byte[] headerBuffer = new byte[headerSize];

            t0 = System.nanoTime();
            inputStream.seek(0);
            inputStream.read(headerBuffer);
            rt += System.nanoTime() - t0;

            t0 = System.nanoTime();
            outputStream.write(headerBuffer);
            wt += System.nanoTime() - t0;

            numBytes += headerBuffer.length;

            int rasterHeight = productFile.getSceneRasterHeight();
            for (int y = 0; y < rasterHeight; y++) {
                for (int i = 0; i < recordReaders.length; i++) {
                    RecordReader recordReader = recordReaders[i];
                    DSD dsd = recordReader.getDSD();
                    long pos = dsd.getDatasetOffset() + (y * dsd.getRecordSize());

                    byte[] recordBuffer = recordBuffers[i];

                    t0 = System.nanoTime();
                    inputStream.seek(pos);
                    inputStream.read(recordBuffer);
                    rt += System.nanoTime() - t0;

                    t0 = System.nanoTime();
                    outputStream.write(recordBuffer);
                    wt += System.nanoTime() - t0;

                    numBytes += recordBuffer.length;
                }
            }

            return new FormatPerformanceMetrics(numBytes, rt, numBytes, wt);

        } finally {
            productFile.close();
        }

    }

    @Override
    public FormatPerformanceMetrics convertFrom(InputStream inputStream, File outputFile) throws IOException {

        FileCacheImageInputStream imageInputStream = new FileCacheImageInputStream(inputStream, new File("."));
        ProductFile productFile = ProductFile.open(imageInputStream);

        try {
            long numBytes = 0;
            long t0;
            long rt = 0;
            long wt = 0;

            RecordReader[] recordReaders = getMdsRecordReaders(productFile);
            byte[][] recordBuffers = getMdsRecordBuffers(recordReaders);

            ImageOutputStream imageOutputStream = new FileImageOutputStream(outputFile);
            int headerSize = (int) recordReaders[0].getDSD().getDatasetOffset();

            byte[] headerBuffer = new byte[headerSize];

            t0 = System.nanoTime();
            imageInputStream.seek(0);
            imageInputStream.read(headerBuffer);
            rt += System.nanoTime() - t0;

            t0 = System.nanoTime();
            imageOutputStream.write(headerBuffer);
            wt += System.nanoTime() - t0;

            numBytes += headerBuffer.length;

            int rasterHeight = productFile.getSceneRasterHeight();
            for (int y = 0; y < rasterHeight; y++) {
                for (int i = 0; i < recordReaders.length; i++) {
                    RecordReader recordReader = recordReaders[i];
                    DSD dsd = recordReader.getDSD();
                    long pos = dsd.getDatasetOffset() + (y * dsd.getRecordSize());

                    byte[] recordBuffer = recordBuffers[i];

                    t0 = System.nanoTime();
                    imageInputStream.read(recordBuffer);
                    imageOutputStream.seek(pos);
                    rt += System.nanoTime() - t0;

                    t0 = System.nanoTime();
                    imageOutputStream.write(recordBuffer);
                    wt += System.nanoTime() - t0;

                    numBytes += recordBuffer.length;
                }
            }

            return new FormatPerformanceMetrics(numBytes, rt, numBytes, wt);

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
}
