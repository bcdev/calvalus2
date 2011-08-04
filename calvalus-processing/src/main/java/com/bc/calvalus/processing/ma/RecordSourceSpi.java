package com.bc.calvalus.processing.ma;

import java.util.Map;
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
     * @param config The configuration
     * @return The record source, or {@code null} if it could not be created.
     */
    public abstract RecordSource createRecordSource(Map<String, String> config) throws Exception;

    /**
     * Gets a SPI instance for the given SPI class name.
     *
     * @param className The SPI class name.
     * @return The SPI instance, or {@code null} if {@code className} is not the name of a registered SPI.
     */
    public static RecordSourceSpi get(String className) {
        ServiceLoader<RecordSourceSpi> loader = ServiceLoader.load(RecordSourceSpi.class, Thread.currentThread().getContextClassLoader());
        for (RecordSourceSpi spi : loader) {
            if (spi.getClass().getName().equals(className)) {
                return spi;
            }
        }
        return null;
    }
}
