package com.bc.calvalus.reporting.restservice.ws;

/**
 * @author hans
 */
class PriceCalculator {

    private final static double CPU_PER_THREAD_PER_HOUR_EURO = 0.001322;

    private final static double MEMORY_PER_GB_PER_HOUR_EURO = 0.000223;

    private final static double DISK_PER_GB_PER_YEAR_EURO = 0.0112;
    private final static double DISK_PER_GB_PER_HOUR_EURO = 0.00000128;

    static double getCpuPrice(long totalVCoresUsedSeconds) {
        double totalVCoresUsedHours = totalVCoresUsedSeconds / 3600;
        return totalVCoresUsedHours * CPU_PER_THREAD_PER_HOUR_EURO;
    }

    static double getMemoryPrice(long gbMillisMapsReduces) {
        double totalMemoryUsedGbSeconds = (gbMillisMapsReduces) / 1024;
        double totalMemoryUsedGbHours = totalMemoryUsedGbSeconds / 3600;
        return totalMemoryUsedGbHours * MEMORY_PER_GB_PER_HOUR_EURO;
    }

    static double getDiskPrice(long fileHdfsBytesReadWriteMb) {
        double totalFileReadWriteGb = fileHdfsBytesReadWriteMb / 1024;
        return totalFileReadWriteGb * DISK_PER_GB_PER_YEAR_EURO;
    }

}
