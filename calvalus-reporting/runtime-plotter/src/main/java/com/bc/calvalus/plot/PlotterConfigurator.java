package com.bc.calvalus.plot;

import java.text.ParseException;
import java.util.Scanner;

public class PlotterConfigurator {
    //Singleton
    private static PlotterConfigurator plotterConfigurator;
    private String inputFile;
    private String category;
    private String colouredDimension; //series
    private String categorySorting;
    private int numberOfSeries;
    private int numberOfCategories;
    private int numberOfSeriesToBeShown;
    private long start = TimeUtils.TIME_NULL;
    private long stop = TimeUtils.TIME_NULL;

    private PlotterConfigurator() {
        super();
    }

    public static PlotterConfigurator getInstance() {
        if (plotterConfigurator == null) {
            plotterConfigurator = new PlotterConfigurator();
        }
        return plotterConfigurator;
    }

    public String getCategory() {
        return category;
    }

    public String getColouredDimension() {
        return colouredDimension;
    }

    public int getNumberOfSeriesToBeShown() {
        return numberOfSeriesToBeShown;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setNumberOfSeries(int numberOfSeries) {
        this.numberOfSeries = numberOfSeries;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setColouredDimension(String colouredDimension) {
        this.colouredDimension = colouredDimension;
    }

    public void askForNumberOfSeriesToBeShown() {
        System.out.println("Number of " + colouredDimension + "s found in the log file: " + numberOfSeries);
        numberOfSeriesToBeShown = askForANumber("How many " + colouredDimension + "s should be shown? ");
    }

    private static int askForANumber(final String question) {
        Scanner in = new Scanner(System.in);
        System.out.println(question);
        final String input = in.next();
        if (!input.matches("\\d{1,3}")) {
            throw new IllegalArgumentException("Expecting an positive integer below 1000.");
        }
        return Integer.valueOf(input);
    }

    public void askForLogFile() {
        Scanner in = new Scanner(System.in);
        System.out.println("Please enter the full name and path of the log file you want to plot. " +
                "There are 2 defaults: type \"default\" or \"errors\".");
        String input = in.next();
        final String userHomeTemp = System.getProperty("user.home") + "/temp/calvalus/";
        if (input.equals("default")) {
            input = userHomeTemp + "hadoop-hadoop-jobtracker-cvmaster00.log.2010-10-28";
        } else if (input.equals("errors")) {
            input = userHomeTemp + "hadoop-hadoop-jobtracker-cvmaster00.log.2010-10-20";
        }
        inputFile = input;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getStop() {
        return stop;
    }

    public void setStop(long stop) {
        this.stop = stop;
    }

    public int getNumberOfSeries() {
        return numberOfSeries;
    }

    public int getNumberOfCategories() {
        return numberOfCategories;
    }

    public void setNumberOfCategories(int numberOfCategories) {
        this.numberOfCategories = numberOfCategories;
    }

    public void configureStartAndStop(String scannerStart, String scannerStop) {
        long logStart = TimeUtils.TIME_NULL;
        long logStop = TimeUtils.TIME_NULL;
        try {
            logStart = TimeUtils.parseCcsdsLocalTimeWithoutT(scannerStart);
            logStop = TimeUtils.parseCcsdsLocalTimeWithoutT(scannerStop);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (logStart != TimeUtils.TIME_NULL &&
                (start == TimeUtils.TIME_NULL || start < logStart)) {
            start = logStart;
        }
        if (logStop != TimeUtils.TIME_NULL &&
                (stop == TimeUtils.TIME_NULL || stop > logStop)) {
            stop = logStop;
        }
    }

    public void setCategorySorting(String categorySorting) {
        this.categorySorting = categorySorting;
    }

    public String getCategorySorting() {
        return categorySorting;
    }
}
