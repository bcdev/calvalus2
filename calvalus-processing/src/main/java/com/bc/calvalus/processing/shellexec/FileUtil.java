package com.bc.calvalus.processing.shellexec;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Utility for file content handling
 *
 * @author Martin Boettcher
 */
public class FileUtil {

    /**
     * @param path  file location
     * @return  file content as string
     * @throws IOException if file does not exist or reading fails
     */
    public static String readFile(String path) throws IOException {
        File file = new File(path);
        Reader in = new FileReader(file);
        try {
            char[] buffer = new char[(int) file.length()];
            in.read(buffer);
            return new String(buffer);
        } finally {
            in.close();
        }
    }
}
