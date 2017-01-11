package com.bc.calvalus.generator.options;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.generator.Launcher;
import com.bc.calvalus.generator.extractor.Extractor;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


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
        initOption(args);
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
            } else {
                displayHelp(commandArg);
            }
        } catch (ParseException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
        } catch (IOException | URISyntaxException e) {
            CalvalusLogger.getLogger().log(Level.SEVERE, e.getMessage());
        }
    }


    private CommandLine createCLIOptions(String[] args) throws ParseException {

        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").build());
        options.addOption(Option.builder("v").longOpt("version").build());
        options.addOption(Option.builder("i").longOpt("interval").hasArg(true).build());
        options.addOption(Option.builder("o").longOpt("output-file-path").hasArg(true).build());

        CommandLineParser commandLineParser = new DefaultParser();
        return commandLineParser.parse(options, args);
    }


    private void startJob(CommandLine commandLine) {
        String intervalS = null;
        if (commandLine.hasOption("i")) {
            intervalS = commandLine.getOptionValue("i");
        } else {
            intervalS = Extractor.createProperties().getProperty("calvalus.history.generate.time.interval");
        }
        int intervalInMinutes = Integer.parseInt(intervalS);
        String urlPath = commandLine.getOptionValue("o");
        Launcher.builder().setUrlPath(urlPath)
                .setTimeIntervalInMinutes(intervalInMinutes)
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
