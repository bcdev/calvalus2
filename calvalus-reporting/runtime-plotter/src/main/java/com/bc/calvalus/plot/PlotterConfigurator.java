package com.bc.calvalus.plot;

import java.util.Scanner;

public class PlotterConfigurator {
    //Singleton
    private static PlotterConfigurator plotterConfigurator;
    private String inputFile;
    private String category;
    private String colouredDimension;
    private int numberOfSeries;
    private int numberOfSeriesToBeShown;

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
        System.out.println("Number of " + colouredDimension + " found in the log file: " + numberOfSeries);
        numberOfSeriesToBeShown = askForANumber("How many " + colouredDimension + " should be shown? ");
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
        System.out.println("Please enter the full name of the log file you want to plot. " +
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
}
