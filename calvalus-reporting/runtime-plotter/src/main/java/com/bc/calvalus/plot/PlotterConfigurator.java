package com.bc.calvalus.plot;

import java.util.Scanner;

public class PlotterConfigurator {
    //Singleton
    private static PlotterConfigurator plotterConfigurator;
    private String category;
    private String colouredDimension;
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

    public void setCategory(String category) {
        this.category = category;
    }

    public void setColouredDimension(String colouredDimension) {
        this.colouredDimension = colouredDimension;
    }

    public void askForNumberOfSeriesToBeShown() {
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
}
