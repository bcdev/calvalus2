package com.bc.calvalus.plot;

import java.util.Scanner;

public class PlotterConfigurator {
    private String category;
    private String colouredDimension;

    public String getCategory() {
        return category;
    }

    public String getColouredDimension() {
        return colouredDimension;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setColouredDimension(String colouredDimension) {
        this.colouredDimension = colouredDimension;
    }

    public static int askForNumberOfJobsToBeShown() {
        return askForANumber("How many jobs should be shown? ");
    }

    public static int askForNumberOfHostsToBeShown() {
        return askForANumber("How many hosts should be shown? ");
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
