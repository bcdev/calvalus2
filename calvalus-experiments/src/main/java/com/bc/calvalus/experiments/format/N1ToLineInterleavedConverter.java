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

    private static final int DEFAULT_TILE_HEIGHT = 1000;

    public FormatPerformanceMetrics convertTo(File inputFile, OutputStream outputStream) throws IOException {

        long numBytes = 0;
        long t0;
        long rt = 0;
        long wt = 0;

        t0 = System.nanoTime();
        ProductFile productFile = ProductFile.open(inputFile);
        try {
            RecordReader[] recordReaders = getMdsRecordReaders(productFile);
            byte[][][] buffers = getMdsRecordBuffers(recordReaders, DEFAULT_TILE_HEIGHT);

            ImageInputStream inputStream = productFile.getDataInputStream();
            int headerSize = (int) recordReaders[0].getDSD().getDatasetOffset();
            rt += System.nanoTime() - t0;

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
            int tileHeight = DEFAULT_TILE_HEIGHT;
            for (int y = 0; y < rasterHeight; y+=tileHeight) {
                if (y + tileHeight > rasterHeight) {
                    tileHeight = rasterHeight - y;
                }
                t0 = System.nanoTime();
                for (int readerIndex = 0; readerIndex < recordReaders.length; readerIndex++) {
                    RecordReader recordReader = recordReaders[readerIndex];

                    DSD dsd = recordReader.getDSD();
                    long pos = dsd.getDatasetOffset() + (y * dsd.getRecordSize());
                    byte[][] recordBuffers = buffers[readerIndex];
                    inputStream.seek(pos);
                    for (byte[] recordLine : recordBuffers) {
                        inputStream.read(recordLine);
                    }
                }
                rt += System.nanoTime() - t0;

                t0 = System.nanoTime();
                for (int line = 0; line < tileHeight; line++) {
                    for (byte[][] recordBuffer : buffers) {
                        byte[] recordLine = recordBuffer[line];
                        outputStream.write(recordLine);

                        numBytes += recordLine.length;
                    }
                }
                wt += System.nanoTime() - t0;
            }
        } finally {
            t0 = System.nanoTime();
            productFile.close();
            wt += System.nanoTime() - t0;
        }
        return new FormatPerformanceMetrics(numBytes, rt, numBytes, wt);
    }

    @Override
    public FormatPerformanceMetrics convertFrom(InputStream inputStream, File outputFile) throws IOException {

        long numBytes = 0;
        long t0;
        long rt = 0;
        long wt = 0;

        FileCacheImageInputStream imageInputStream = new FileCacheImageInputStream(inputStream, new File("."));

        t0 = System.nanoTime();
        ProductFile productFile = ProductFile.open(imageInputStream);
        try {
            RecordReader[] recordReaders = getMdsRecordReaders(productFile);
            byte[][][] recordBuffers = getMdsRecordBuffers(recordReaders, 1);// TODO not optimized, handles singles lines

            ImageOutputStream imageOutputStream = new FileImageOutputStream(outputFile);
            int headerSize = (int) recordReaders[0].getDSD().getDatasetOffset();

            byte[] headerBuffer = new byte[headerSize];

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

                    t0 = System.nanoTime();
                    DSD dsd = recordReader.getDSD();
                    long pos = dsd.getDatasetOffset() + (y * dsd.getRecordSize());
                    byte[] recordBuffer = recordBuffers[i][0];
                    imageInputStream.read(recordBuffer);
                    imageOutputStream.seek(pos);
                    rt += System.nanoTime() - t0;

                    t0 = System.nanoTime();
                    imageOutputStream.write(recordBuffer);
                    wt += System.nanoTime() - t0;

                    numBytes += recordBuffer.length;
                }
            }


        } finally {
            t0 = System.nanoTime();
            productFile.close();
            wt += System.nanoTime() - t0;
        }
        return new FormatPerformanceMetrics(numBytes, rt, numBytes, wt);

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

    private byte[][][] getMdsRecordBuffers(RecordReader[] recordReaders, int tileHeight) {
        byte[][][] recordBuffers = new byte[recordReaders.length][tileHeight][];
        for (int i = 0; i < recordReaders.length; i++) {
            recordBuffers[i] = new byte[tileHeight][];
            for (int tileIndex = 0; tileIndex < tileHeight; tileIndex++) {
                recordBuffers[i][tileIndex] = new byte[recordReaders[i].getDSD().getRecordSize()];
            }
        }
        return recordBuffers;
    }
}
