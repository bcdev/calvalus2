package com.bc.calvados.hadoop.eodata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ProductFileConverter {
    void convertToMRFriendlyFormat(File inputFile, OutputStream outputStream) throws IOException;
    void convertFromMRFriendlyFormat(InputStream inputStream, File outputFile) throws IOException;
}
