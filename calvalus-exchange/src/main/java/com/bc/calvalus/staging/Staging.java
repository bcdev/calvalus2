package com.bc.calvalus.staging;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.ceres.core.CanceledException;
import com.bc.ceres.core.runtime.internal.DirScanner;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public abstract class Staging implements Runnable {

    public static Logger LOGGER = CalvalusLogger.getLogger();

    private boolean cancelled;

    /**
     * Performs the staging.
     */
    @Override
    public abstract void run();

    public void performStaging() throws Throwable {
    }


    public final boolean isCancelled() {
        return cancelled;
    }

    public void cancel() {
        cancelled = true;
    }

    public static void zip(File sourceDir, File targetZipFile) throws IOException, CanceledException {
        if (!sourceDir.exists()) {
            throw new FileNotFoundException(sourceDir.getPath());
        }

        // Important: First scan, ...
        LOGGER.info("Scanning " + sourceDir + "  for files to zip ...");
        DirScanner dirScanner = new DirScanner(sourceDir, true, true);
        String[] entryNames = dirScanner.scan();
        LOGGER.info("Entries found: " + entryNames.length);
        //            ... then create new file (avoid including the new ZIP in the ZIP!)
        if (entryNames.length > 0) {
            ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(targetZipFile)));
            zipOutputStream.setMethod(ZipEntry.DEFLATED);

            try {
                for (String entryName : entryNames) {
                    // also avoid including zip file if it pre-exists
                    if (entryName.equals(targetZipFile.getName())) {
                        continue;
                    }
                    LOGGER.info("Adding " + entryName + " to zip ...");
                    ZipEntry zipEntry = new ZipEntry(entryName.replace('\\', '/'));

                    File sourceFile = new File(sourceDir, entryName);
                    FileInputStream inputStream = new FileInputStream(sourceFile);
                    try {
                        zipOutputStream.putNextEntry(zipEntry);
                        copy(inputStream, zipOutputStream);
                        zipOutputStream.closeEntry();
                    } finally {
                        inputStream.close();
                    }
                }
            } finally {
                zipOutputStream.close();
            }
        }
    }

    private static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[64 * 1024];
        while (true) {
            int n = inputStream.read(buffer);
            if (n > 0) {
                outputStream.write(buffer, 0, n);
            } else if (n < buffer.length) {
                break;
            }
        }
    }

}
