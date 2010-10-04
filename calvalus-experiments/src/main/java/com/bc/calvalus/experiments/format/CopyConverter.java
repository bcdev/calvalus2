package com.bc.calvalus.experiments.format;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CopyConverter implements FileConverter {

    public void convertTo(File inputFile, OutputStream outputStream) throws IOException {
        InputStream inputStream = new FileInputStream(inputFile);
        try {
            copy(inputStream, outputStream);
        } finally {
            inputStream.close();
        }
    }

    @Override
    public void convertFrom(InputStream inputStream, File outputFile) throws IOException {
        OutputStream outputStream = new FileOutputStream(outputFile);
        try {
            copy(inputStream, outputStream);
        } finally {
            outputStream.close();
        }
    }

    public static long copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[1024 * 1024];
        int n;
        long size = 0;
        while ((n = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, n);
            size += n;
        }
        return size;
    }
}
