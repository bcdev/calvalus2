package com.bc.calvalus.processing.ma;

import java.util.ServiceLoader;

/**
 * A source for match-up records.
 *
 * @author Norman
 */
public abstract class RecordSourceSpi {
    public abstract RecordSource createRecordSource(MAConfig maConfig);

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
