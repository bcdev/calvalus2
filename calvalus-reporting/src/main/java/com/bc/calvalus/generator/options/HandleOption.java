package com.bc.calvalus.generator.options;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.generator.Launcher;
import com.bc.wps.utilities.PropertiesWrapper;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


/**
 * @author muhammad.bc.
 */
public class HandleOption extends PrintOption {

    private static final String HELP_OPTION = "h";
    private static final String VERSION_OPTION = "v";
    private static final String START_OPTION = "start";
    private static final String INTERVAL_OPTION = "i";
    private static final String CALVALUS_HISTORY_GENERATE_TIME_INTERVAL_DEFAULT = "calvalus.history.generate.time.interval.default";
    private static final String VERSION = "version";
    private static final String OUTPUT_OPTION = "o";
    private CommandLine commandLine;

    public HandleOption(String args[]) {
        if (args.length == 0) {
            printMsg("Please specify a parameter, for more detail type '-h'");
            return;
        }
        initOption(args);
    }

    String getOptionValue(String option) {
        return commandLine.getOptionValue(option);
    }

    CommandLine getCommandLine() {
        return commandLine;
    }

    private void initOption(String[] args) {
        try {
            CommandLineParser commandLineParser = new DefaultParser();

            Options options = createAllOptions();
            commandLine = commandLineParser.parse(options, args);
            String firstArg = args[0];
            if (commandLine.hasOption(HELP_OPTION)) {
                displayHelp(firstArg, options);
                return;
            } else if (commandLine.hasOption(VERSION_OPTION)) {
                displayVersion();
                return;
            }

            if (firstArg.equalsIgnoreCase(START_OPTION)) {
                startJob(commandLine);
            }
        } catch (ParseException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
            printErrorMsg("");
        }
    }


    private void startJob(CommandLine commandLine) {
        String intervalS;
        if (commandLine.hasOption(INTERVAL_OPTION)) {
            intervalS = commandLine.getOptionValue(INTERVAL_OPTION);
        } else {
            intervalS = PropertiesWrapper.get(CALVALUS_HISTORY_GENERATE_TIME_INTERVAL_DEFAULT);
        }
        int intervalInMinutes = Integer.parseInt(intervalS);
        String urlPath = commandLine.getOptionValue(OUTPUT_OPTION);
        Launcher.builder().setUrlPath(urlPath)
                .setTimeIntervalInMinutes(intervalInMinutes)
                .start();
    }

    private void displayVersion() {
        String version = PropertiesWrapper.get(VERSION);
        String format = String.format("Calvalus Generator version %s.", version);
        printMsg(format);
    }

    private void displayHelp(String command, Options options) {
        if (command.equalsIgnoreCase(START_OPTION)) {
            printStartHelp();
        } else {
            printHelp(options);
        }
    }

}
