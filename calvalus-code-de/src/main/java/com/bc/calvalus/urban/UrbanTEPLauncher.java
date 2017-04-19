package com.bc.calvalus.urban;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.wps.utilities.PropertiesWrapper;
import com.jcraft.jsch.Session;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author muhammad.bc.
 */
public class UrbanTEPLauncher {
    private Logger log = CalvalusLogger.getLogger();

    public UrbanTEPLauncher() {
        try {
            PropertiesWrapper.loadConfigFile("urbanTEP.properties");
            SCPCommand scpCommand = new SCPCommand();
            Optional<Session> session = scpCommand.getSession();
            Optional<BufferedReader> bufferedReaderOptional = scpCommand.readFile(session.get());
            BufferedReader bufferedReader = bufferedReaderOptional.get();

            String s = bufferedReader.readLine();
            System.out.println("s = " + s);


        } catch (IOException e) {
            log.log(Level.SEVERE, e.getMessage());
        }
    }

    public static void main(String[] args) {
        new UrbanTEPLauncher();
    }
}
