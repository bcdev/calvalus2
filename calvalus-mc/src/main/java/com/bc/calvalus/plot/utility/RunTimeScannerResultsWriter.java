package com.bc.calvalus.plot.utility;

import com.bc.calvalus.plot.RunTimesScanner;
import com.bc.calvalus.plot.Trace;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RunTimeScannerResultsWriter {
    private final static Logger LOGGER = Logger.getAnonymousLogger();

    /**
     * To play around and look on scan results in file
     *  **/
    public static void main(String[] args) {
        fetchInputLogFiles();
    }

    /* just to analyse the results -- not a part of the code */
    private static void fetchInputLogFiles() {
        final String userHomeTemp = System.getProperty("user.home") + "/temp/calvalus/";
        final String fileName = "hadoop-hadoop-jobtracker-master00.log.2010-10-28";  //all STATUS done
//        final String fileName = "hadoop-hadoop-jobtracker-master00.log.2010-10-20";  //some failed, some open
        RunTimesScanner runTimesScanner;
        BufferedWriter bufferedWriter;
        try {
            runTimesScanner = new RunTimesScanner(new BufferedReader(new FileReader(userHomeTemp + fileName)));
            runTimesScanner.scan();

            bufferedWriter = new BufferedWriter(new FileWriter(userHomeTemp + "RunTimeScannerResults.txt"));
            bufferedWriter.write("scanner start: " + runTimesScanner.getStart() + "\n");
            bufferedWriter.write("scanner stop: " + runTimesScanner.getStop() + "\n");
            for (Trace trace : runTimesScanner.getTraces()) {
                bufferedWriter.write((trace.isOpen() ? "*" : "") + trace.toString() + "\n");
            }
            bufferedWriter.write(runTimesScanner.getValids().toString() + "\n");
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, "could not find file in " + userHomeTemp, e);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "error opening FileWriter ", e);
        }
    }

}
