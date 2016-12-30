package com.bc.calvalus.reporting;

import java.text.DecimalFormat;

/**
 * @author hans
 */
class PriceCalculator {

    private final static float CPU_PER_THREAD_PER_HOUR_EURO = 0.001322f;

    private final static float MEMORY_PER_GB_PER_HOUR_EURO = 0.000223f;

    private final static float DISK_PER_GB_PER_YEAR_EURO = 0.0112f;
    private final static float DISK_PER_GB_PER_HOUR_EURO = 0.00000128f;

    static double getCpuPrice(long vCoresMillisMaps, long vCoresMillisReduces) {
        double totalVCoresUsedSeconds = (vCoresMillisMaps + vCoresMillisReduces) / 1000;
        double totalVCoresUsedHours = totalVCoresUsedSeconds / 3600;
        return monetize(totalVCoresUsedHours * CPU_PER_THREAD_PER_HOUR_EURO);
    }

    static double getMemoryPrice(long mbMillisMaps, long mbMillisReduces) {
        double totalMemoryUsedGbSeconds = (mbMillisMaps + mbMillisReduces) / (1000 * 1024);
        double totalMemoryUsedGbHours = totalMemoryUsedGbSeconds / 3600;
        return monetize(totalMemoryUsedGbHours * MEMORY_PER_GB_PER_HOUR_EURO);
    }

    static double getDiskPrice(long fileBytesRead, long fileBytesWrite, long hdfsBytesRead, long hdfsBytesWrite) {
        double totalFileReadGb = (fileBytesRead + hdfsBytesRead) / (1024 * 1024 * 1024);
        double totalFileWriteGb = (fileBytesWrite + hdfsBytesWrite) / (1024 * 1024 * 1024);
        double totalReadWriteGb = totalFileReadGb + totalFileWriteGb;
        return monetize(totalReadWriteGb * DISK_PER_GB_PER_YEAR_EURO);
    }

    private static double monetize(double rawDoubleValue) {
        DecimalFormat df = new DecimalFormat("#.00");
        return Double.parseDouble(df.format(rawDoubleValue));
    }
}
