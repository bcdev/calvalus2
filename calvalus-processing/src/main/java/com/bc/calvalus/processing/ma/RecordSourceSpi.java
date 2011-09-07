package com.bc.calvalus.processing.ma;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.net.URL;
import java.util.ServiceLoader;

/**
 * The service provider interface (SPI) for record sources.
 *
 * @author MarcoZ
 * @author Norman
 */
public abstract class RecordSourceSpi {

    static {
        try {
            // Make "hdfs:" a recognised URL protocol
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Throwable e) {
            // ignore as it is most likely already set
            String msg = String.format("Cannot set URLStreamHandlerFactory (message: '%s'). " +
                                               "This may not be a problem because it is most likely already set.",
                                       e.getMessage());
            CalvalusLogger.getLogger().fine(msg);
        }
    }

    /**
     * Creates a new record source using the given configuration.
     *
     * @param recordSourceUrl The URL of the record source.
     * @return The record source, or {@code null} if it could not be created.
     * @throws Exception If an error occurs.
     */
    public abstract RecordSource createRecordSource(String recordSourceUrl) throws Exception;

    /**
     * Checks if the content identified by the given URL can be decoded by this SPI.
     * A simple test could be to just check for known filename extensions.
     *
     * @param recordSourceUrl A record source URL.
     * @return {@code true}, if this SPI can decode the content of the given URL.
     */
    protected abstract boolean canDecodeContent(String recordSourceUrl);

    /**
     * Gets a SPI instance for the given SPI class name.
     *
     * @param className The SPI class name.
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
