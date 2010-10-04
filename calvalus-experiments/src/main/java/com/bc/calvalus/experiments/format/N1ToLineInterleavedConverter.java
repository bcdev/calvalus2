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
 */
public class N1ToLineInterleavedConverter implements FileConverter {

    public void convertTo(File inputFile, OutputStream outputStream) throws IOException {
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
    public void convertFrom(InputStream inputStream, File outputFile) throws IOException {

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
}
