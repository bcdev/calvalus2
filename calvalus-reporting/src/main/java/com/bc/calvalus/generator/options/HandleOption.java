package com.bc.calvalus.generator.options;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.generator.Launcher;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;


/**
 * @author muhammad.bc.
 */
public class HandleOption extends PrintOption {

    private CommandLine commandLine;

    public HandleOption(String args[]) {
        if (args.length == 0 || args == null) {
            printMsg("Please specify a parameter, for more detail type '-h'");
            return;
        }
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

            if (commandArg.equalsIgnoreCase("start") && checkToStart()) {
                startJob(commandLine);
            } else {
                displayHelp(commandArg);
            }
        } catch (ParseException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private boolean checkToStart() {
        if (commandLine.getOptionValue("i") != null && commandLine.getOptionValue("o") != null) {
            return true;
        }
        return false;
    }

    private boolean confirmOption(String toString) {
        boolean confirm = (toString.contains("start") && toString.contains("stop"));
        return confirm;
    }


    private CommandLine createCLIOptions(String[] args) throws ParseException {

        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").build());
        options.addOption(Option.builder("v").longOpt("version").build());
        options.addOption(Option.builder("i").longOpt("interval").hasArg(true).build());
        options.addOption(Option.builder("o").longOpt("output-path").hasArg(true).build());

        CommandLineParser commandLineParser = new DefaultParser();
        return commandLineParser.parse(options, args);
    }


    private void startJob(CommandLine commandLine) {
        Launcher.builder().setUrlPath(commandLine.getOptionValue("o"))
                .setTimeIntervalInMinutes(Integer.parseInt(commandLine.getOptionValue("i")))
                .start();

    }

    private void displayVersion() {
        Properties buildProperties = getBuildProperties();
        String version = (String) buildProperties.get("version");
        String format = String.format("Calvalus Generator version %s.", version);
        printMsg(format);
    }

    private void displayHelp(String command) throws IOException, URISyntaxException {
        if ("start".equalsIgnoreCase(command)) {
            printHelp(HELP_START);
        } else {
            printHelp(HELP_INFO);
        }
    }


}
