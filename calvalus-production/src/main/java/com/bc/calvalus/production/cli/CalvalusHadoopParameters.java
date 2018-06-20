package com.bc.calvalus.production.cli;

import org.apache.hadoop.conf.Configuration;

public class CalvalusHadoopParameters extends Configuration {

    public CalvalusHadoopParameters() {
        super(false);
    }

    /**
     * Function for use in production type translation rules
     */
    public String seconds2Millis(String seconds) {
        return seconds + "000";
    }

    /**
     * Function for use in production type translation rules
     */
    public String javaOptsOfMem(String mem) {
        return "-Djava.awt.headless=true -Xmx" + mem + "M";
    }

    /**
     * Function for use in production type translation rules
     */
    public String javaOptsForExec(String mem) {
        return "-Djava.awt.headless=true -Xmx384M";
    }

    /**
     * Function for use in production type translation rules
     */
    public String add512(String mem) {
        return String.valueOf(Integer.parseInt(mem) + 512);
    }

    /**
     * Function for use in production type translation rules
     */
    public String minDateOf(String dateRanges) {
        return dateRanges.substring(1, dateRanges.length() - 1).split(":")[0].trim();
    }

    /**
     * Function for use in production type translation rules
     */
    public String maxDateOf(String dateRanges) {
        return dateRanges.substring(1, dateRanges.length() - 1).split(":")[1].trim();
    }

    /**
     * Function for use in production type translation rules
     */
    public String minMaxDateRange(String minDate) {
        String dateRanges = get("calvalus.input.dateRanges");
        if (dateRanges != null) {
            return String.format(minDate, dateRanges.split(":")[1].trim());
        } else {
            return String.format(minDate);
        }
    }

    /**
     * Function for use in production type translation rules
     */
    public String maxMinDateRange(String maxDate) {
        String dateRanges = get("calvalus.input.dateRanges");
        if (dateRanges != null) {
            return String.format(dateRanges.split(":")[0].trim(), maxDate);
        } else {
            return String.format(maxDate);
        }
    }

    /**
     * Function for use in production type translation rules
     */
    public String listDateRange(String date) {
        return String.format(date, date);
    }
}