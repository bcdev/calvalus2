package com.bc.calvados.hadoop.eodata;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public interface ProductFileConverter {
     void convert(File inputFile, OutputStream outputStream) throws IOException;
}
