package com.bc.calvalus.ogcapiprocesses.model;

import com.bc.calvalus.production.cli.CalvalusHadoopConnection;

import java.io.File;
import java.io.IOException;

public class ProcessDescriptorOld {
    private static final String SYSTEM_PATH = "/calvalus/software/1.0";
    private static final String USER_HOME = "/calvalus/home";

    private final String serviceDir;
    private final String processorName;
    private final String processID;

    public ProcessDescriptorOld(String processorName, String processID, String serviceDir) {
        this.serviceDir = serviceDir;
        this.processorName = processorName;
        this.processID = processID;
    }

    public String getProcessID() {
        return processID;
    }
    public String getProcessorName() {
        return processorName;
    }

    private String relativePathString(boolean skipExtension) {
        return processorName + File.separator + processID +  (skipExtension ? "" : "-descriptor.json");
    }

    public String relativePathString() {
        return relativePathString(false);
    }

    /**
     * @return The path to the local copy of the processor descriptor
     */
    public String localPathString() {
        return serviceDir + File.separator + relativePathString();
    }

    /**
     * @return The path to the local copy of the processor descriptor
     */
    public File localPath() {
        return new File(localPathString());
    }

    public String getContent(CalvalusHadoopConnection hadoopConnection, String username) {
        try {
            String processorName = relativePathString(true);
            return hadoopConnection.getProcessorDescriptor(processorName, username);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Could not read processor descriptor " + processorName, e);
        }

    }


}
