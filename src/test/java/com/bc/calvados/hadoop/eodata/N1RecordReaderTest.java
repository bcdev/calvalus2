package com.bc.calvados.hadoop.eodata;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class N1RecordReaderTest {
    @Test
    public void testN1() throws IOException {
        File outputFile = new File("src/test/data/reformat");
        FileOutputStream outputStream = new FileOutputStream(outputFile);

        File inputFile = new File("src/test/data/MER_RR__1P.N1");
        assertTrue(inputFile.length() > 0);

        ProductFileConverter n1Converter = new N1ProductFileConverter();
        n1Converter.convert(inputFile, outputStream);
        outputStream.close();

        assertEquals(outputFile.length(), inputFile.length());
    }
}
