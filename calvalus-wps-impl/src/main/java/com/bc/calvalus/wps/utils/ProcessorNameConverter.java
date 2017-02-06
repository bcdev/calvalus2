package com.bc.calvalus.wps.utils;

import com.bc.calvalus.wps.calvalusfacade.CalvalusProcessor;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import org.apache.commons.lang.StringUtils;

/**
 * @author hans
 */
public class ProcessorNameConverter {

    private String bundleName;
    private String bundleVersion;
    private String executableName;
    private String processorIdentifier;

    public ProcessorNameConverter(String processorIdentifier) throws InvalidProcessorIdException {
        this.processorIdentifier = processorIdentifier;
        parseProcessorId();
    }

    public ProcessorNameConverter(String bundleName, String bundleVersion, String executableName){
        this.bundleName = bundleName;
        this.bundleVersion = bundleVersion;
        this.executableName = executableName;
        constructProcessorId();
    }

    public String getBundleName() {
        return bundleName;
    }

    public String getBundleVersion() {
        return bundleVersion;
    }

    public String getExecutableName() {
        return executableName;
    }

    public String getProcessorIdentifier() {
        return processorIdentifier;
    }

    private void parseProcessorId() throws InvalidProcessorIdException {
        if (!StringUtils.isBlank(processorIdentifier)) {
            String parsedString[] = processorIdentifier.split(CalvalusProcessor.DELIMITER);
            if (parsedString.length < 3) {
                throw new InvalidProcessorIdException(processorIdentifier);
            }
            this.bundleName = parsedString[0];
            this.bundleVersion = parsedString[1];
            this.executableName = parsedString[2];
        } else {
            throw new InvalidProcessorIdException(processorIdentifier);
        }
    }

    private void constructProcessorId(){
        this.processorIdentifier = bundleName.concat("~").concat(bundleVersion).concat("~").concat(executableName);
    }
}
