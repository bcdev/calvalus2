package com.bc.calvalus.experiments.format;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CopyConverter implements FileConverter {

    public FormatPerformanceMetrics convertTo(File inputFile, OutputStream outputStream) throws IOException {
        InputStream inputStream = new FileInputStream(inputFile);
        try {
            return copy(inputStream, outputStream);
        } finally {
            inputStream.close();
        }
    }

    @Override
    public FormatPerformanceMetrics convertFrom(InputStream inputStream, File outputFile) throws IOException {
        OutputStream outputStream = new FileOutputStream(outputFile);
        try {
            return copy(inputStream, outputStream);
        } finally {
            outputStream.close();
        }
    }

    public static FormatPerformanceMetrics copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[1024 * 1024];
        long numBytes = 0;
        long rt = 0;
        long wt = 0;
        long t0;
        while (true) {

            t0 = System.nanoTime();
            int n = inputStream.read(buffer);
            rt += System.nanoTime() - t0;

            if (n <= 0) {
                break;
            }

            t0 = System.nanoTime();
            outputStream.write(buffer, 0, n);
            wt += System.nanoTime() - t0;

            numBytes += n;
        }

        return new FormatPerformanceMetrics(numBytes, rt, numBytes, wt);
    }
}
