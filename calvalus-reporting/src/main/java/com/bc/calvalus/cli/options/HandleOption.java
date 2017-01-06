package com.bc.calvalus.cli.options;


import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;


/**
 * @author muhammad.bc.
 */
public class HandleOption extends PrintOption {

    private CommandLine commandLine;

    public HandleOption(String args[]) {
        String toString = Arrays.toString(args);
        boolean confirmOption = confirmOption(toString);
        if (!confirmOption) {
            initOption(args);
        } else {
            printErrorMsg(toString);
        }
    }
    public String getOptionValue(String option) {
        return commandLine.getOptionValue(option);
    }

    public CommandLine getCommandLine() {
        return commandLine;
    }

    private void initOption(String[] args) {
        try {
            commandLine = createCLIOptions(args);
            String commandArg = args[0];
            if (commandLine.hasOption("h")) {
                displayHelp(commandArg);
                return;
            } else if (commandLine.hasOption("v")) {
                displayVersion();
                return;
            }

            if (commandArg.equalsIgnoreCase("start")) {
                startJob(commandLine);
            } else if (commandArg.equalsIgnoreCase("stop")) {
                stopJob();
            }


        } catch (ParseException e) {
            if (e instanceof UnrecognizedOptionException) {
                String option = ((UnrecognizedOptionException) e).getOption();
                printErrorMsg(option);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean confirmOption(String toString) {
        boolean confirm = (toString.contains("start") && toString.contains("stop"));
        return confirm;
    }



    private CommandLine createCLIOptions(String[] args) throws ParseException {

        Options options = new Options();
        options.addOption(Option.builder("f").longOpt("force").build());
        options.addOption(Option.builder("h").longOpt("help").build());
        options.addOption(Option.builder("i").longOpt("interval").hasArg(true).build());
        options.addOption(Option.builder("u").longOpt("job-url").hasArg(true).build());
        options.addOption(Option.builder("o").longOpt("output-path").hasArg(true).build());
        options.addOption(Option.builder("v").longOpt("version").build());

        CommandLineParser commandLineParser = new DefaultParser();
        return commandLineParser.parse(options, args);
    }

    private void stopJob() {

    }

    private void startJob(CommandLine commandLine) {

    }

    private void displayVersion() {
        Properties buildProperties = getBuildProperties();
        String version = (String) buildProperties.get("version");
        String format = String.format("Calvalus Generator version %s.", version);
        printMsg(format);
    }

    private void displayHelp(String command) throws IOException {
        if ("start".equalsIgnoreCase(command)) {
            printHelp("help_start.txt");
        } else if ("stop".equalsIgnoreCase(command)) {
            printHelp("help_stop.txt");
        } else if ("interval".equalsIgnoreCase(command)) {
            printHelp("help_interval.txt");
        } else if ("save-location".equalsIgnoreCase(command)) {
            printHelp("help_location.txt");
        } else if ("job-url".equalsIgnoreCase(command)) {
            printHelp("help_job_url.txt");
        } else {
            printHelp("help_info.txt");
        }
    }


}
