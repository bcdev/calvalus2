package com.bc.calvalus.experiments.format;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FileConverter {
    void convertTo(File inputFile, OutputStream outputStream) throws IOException;

    void convertFrom(InputStream inputStream, File outputFile) throws IOException;
}
