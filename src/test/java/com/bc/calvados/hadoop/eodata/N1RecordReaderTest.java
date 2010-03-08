package com.bc.calvados.hadoop.eodata;

import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.*;

public class N1RecordReaderTest {
    private static final int ONE_MB = 1024*1024;

    @Test
    public void testConvertToAndFrom() throws IOException {
        File convertedFile = new File("src/test/data/converted");
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(convertedFile), ONE_MB);

        File inputFile = new File("src/test/data/MER_RR__1P.N1");
        assertTrue(inputFile.length() > 0);

        ProductFileConverter n1Converter = new N1ProductFileConverter();
        n1Converter.convertToMRFriendlyFormat(inputFile, outputStream);
        outputStream.close();

        InputStream inputStream = new BufferedInputStream(new FileInputStream(convertedFile), ONE_MB);
        File reconvertedFile = new File("src/test/data/reconverted.N1");
        n1Converter.convertFromMRFriendlyFormat(inputStream, reconvertedFile);
        inputStream.close();

        assertEquals(convertedFile.length(), inputFile.length());
        assertEquals(reconvertedFile.length(), convertedFile.length());

        assertTrue(compare(inputFile, convertedFile) != 0);
        assertTrue(compare(convertedFile, reconvertedFile) != 0);
        assertTrue(compare(inputFile, reconvertedFile) == 0);
    }

    private int compare(File file1, File file2) throws IOException {
        InputStream stream1 = new BufferedInputStream(new FileInputStream(file1), ONE_MB);
        InputStream stream2 =  new BufferedInputStream(new FileInputStream(file2), ONE_MB);

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
