package com.bc.calvalus.processing.ma;

import java.io.IOException;
import java.io.OutputStream;

/**
* Factory for output streams. Used to make file-based code testable.
*
* @author Norman
*/
public interface OutputStreamFactory {
    OutputStream createOutputStream(String path) throws IOException, InterruptedException;
}
