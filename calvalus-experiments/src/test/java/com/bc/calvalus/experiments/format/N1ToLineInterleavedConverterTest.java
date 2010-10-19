package com.bc.calvalus.experiments.format;

import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class N1ToLineInterleavedConverterTest {
    private static final int ONE_MB = 1024 * 1024;
    private static final String INPUT_PATH = "/MER_RR__1P.N1";

    @Test
    public void testConvertToAndFrom() throws IOException {
        File testDataDir = new File("target/testdata");
        testDataDir.mkdirs();
        File convertedFile = new File(testDataDir, "converted");
        //convertedFile.deleteOnExit();
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(convertedFile), ONE_MB);

        final String path = getClass().getResource(INPUT_PATH).getPath();
        File inputFile = new File(path);
        assertTrue(inputFile.length() > 0);

        FileConverter n1Converter = new N1ToLineInterleavedConverter();
        n1Converter.convertTo(inputFile, outputStream);
        outputStream.close();

        InputStream inputStream = new BufferedInputStream(new FileInputStream(convertedFile), ONE_MB);
        File reconvertedFile = new File(testDataDir, "reconverted.N1");
        n1Converter.convertFrom(inputStream, reconvertedFile);
        inputStream.close();

        assertEquals(convertedFile.length(), inputFile.length());
        assertEquals(reconvertedFile.length(), convertedFile.length());

        assertTrue(compare(inputFile, convertedFile) != 0);
        assertTrue(compare(convertedFile, reconvertedFile) != 0);
        assertTrue(compare(inputFile, reconvertedFile) == 0);
    }

    private int compare(File file1, File file2) throws IOException {
        InputStream stream1 = new BufferedInputStream(new FileInputStream(file1), ONE_MB);
        InputStream stream2 = new BufferedInputStream(new FileInputStream(file2), ONE_MB);

        int b1, b2;
        while ((b1 = stream1.read()) >= 0) {
            b2 = stream2.read();
            if (b1 != b2) {
                return b1 - b2;
            }
        }

        stream1.close();
        stream2.close();

        return 0;
    }
}
