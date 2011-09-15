package com.bc.calvalus.staging;

import com.bc.ceres.core.CanceledException;
import com.bc.ceres.core.runtime.internal.DirScanner;

import java.io.*;
import java.util.concurrent.Callable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public abstract class Staging implements Callable<Object> {
    private boolean cancelled;

    /**
     * Performs the staging.
     *
     * @return The relative staging path.
     * @throws Exception if an error occurs.
     */
    @Override
    public abstract Object call() throws Exception;

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
        DirScanner dirScanner = new DirScanner(sourceDir, true, true);
        String[] entryNames = dirScanner.scan();
        //            ... then create new file (avoid including the new ZIP in the ZIP!)
        ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(targetZipFile)));
        zipOutputStream.setMethod(ZipEntry.DEFLATED);

        try {
            for (String entryName : entryNames) {
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

    private static void copy(InputStream inputStream, OutputStream outputStream) throws IOException{
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
