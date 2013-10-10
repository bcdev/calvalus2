package com.bc.calvalus.processing.ma;

import java.util.ServiceLoader;

/**
 * The service provider interface (SPI) for record sources.
 *
 * @author MarcoZ
 * @author Norman
 */
public abstract class RecordSourceSpi {

    /**
     * Creates a new record source using the given configuration.
     *
     * @param recordSourceUrl The URL of the record source.
     *
     * @return The record source, or {@code null} if it could not be created.
     *
     * @throws Exception If an error occurs.
     */
    public abstract RecordSource createRecordSource(String recordSourceUrl) throws Exception;

    /**
     * Checks if the content identified by the given URL can be decoded by this SPI.
     * A simple test could be to just check for known filename extensions.
     *
     * @param recordSourceUrl A record source URL.
     *
     * @return {@code true}, if this SPI can decode the content of the given URL.
     */
    protected boolean canDecodeContent(String recordSourceUrl) {
        for (String extension : getAcceptedExtensions()) {
            if (recordSourceUrl.endsWith(extension)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Defines the supported extensions of filenames this record source reader can decode.
     * Example: { ".csv", ".txt" }
     *
     * @return array of strings of file name extensions
     */
    public abstract String[] getAcceptedExtensions();

    /**
     * Gets a SPI instance for the given SPI class name.
     *
     * @param className The SPI class name.
     *
     * @return The SPI instance, or {@code null} if {@code className} is not the name of a registered SPI.
     */
    public static RecordSourceSpi getForClassName(String className) {
        ServiceLoader<RecordSourceSpi> loader = ServiceLoader.load(RecordSourceSpi.class, Thread.currentThread().getContextClassLoader());
        for (RecordSourceSpi spi : loader) {
            if (spi.getClass().getName().equals(className)) {
                return spi;
            }
        }
        return null;
    }

    /**
     * Gets a SPI instance for the given URL.
     *
     * @param recordSourceUrl The record source URL.
     *
     * @return The SPI instance, or {@code null} if {@code recordSourceUrl} is not recorgnized by any registered SPI.
     */
    public static RecordSourceSpi getForUrl(String recordSourceUrl) {
        ServiceLoader<RecordSourceSpi> loader = ServiceLoader.load(RecordSourceSpi.class, Thread.currentThread().getContextClassLoader());
        for (RecordSourceSpi spi : loader) {
            if (spi.canDecodeContent(recordSourceUrl)) {
                return spi;
            }
        }
        return null;
    }

}
