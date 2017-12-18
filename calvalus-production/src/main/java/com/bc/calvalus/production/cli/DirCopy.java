package com.bc.calvalus.production.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * @author Norman
 */
public class DirCopy {

    public static void copyDir(File src, FileSystem dstFS, Path dst, Configuration configuration) throws IOException {
        if (src.isDirectory()) {
            FileUtil.copy(src, dstFS, dst, false, configuration);
        } else {
            unzip(src, dstFS, dst);
        }
    }

    /**
     * Given a File input it will unzip the file in a the unzip directory
     * passed as the second parameter
     *
     * @param inFile   The zip file as input
     * @param unzipDir The unzip directory where to unzip the zip file.
     * @throws IOException If an I/O error occurs
     */
    public static void unzip(File inFile, FileSystem fs, Path unzipDir) throws IOException {
        ZipFile zipFile = new ZipFile(inFile);
        try {
            unzip(zipFile, fs, unzipDir);
        } finally {
            zipFile.close();
        }
    }

    /**
     * Given a File input it will unzip the file in a the unzip directory
     * passed as the second parameter
     *
     * @param zipFile  The zip file as input
     * @param unzipDir The unzip directory where to unzip the zip file.
     * @throws IOException If an I/O error occurs
     */
    public static void unzip(ZipFile zipFile, FileSystem fs, Path unzipDir) throws IOException {
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            if (!entry.isDirectory()) {
                InputStream in = zipFile.getInputStream(entry);
                try {
                    Path file = new Path(unzipDir, entry.getName());
                    Path parent = file.getParent();
                    if (!fs.mkdirs(parent)) {
                        if (!fs.getFileStatus(parent).isDirectory()) {
                            throw new IOException("FileSystem.mkdirs() failed to create " + parent.toString());
                        }
                    }
                    OutputStream out = fs.create(file);
                    try {
                        byte[] buffer = new byte[8192];
                        int i;
                        while ((i = in.read(buffer)) != -1) {
                            out.write(buffer, 0, i);
                        }
                    } finally {
                        out.close();
                    }
                } finally {
                    in.close();
                }
            }
        }
    }
}
