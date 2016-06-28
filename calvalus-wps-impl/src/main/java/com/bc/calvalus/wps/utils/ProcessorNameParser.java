package com.bc.calvalus.wps.utils;

import com.bc.calvalus.wps.calvalusfacade.CalvalusProcessor;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import org.apache.commons.lang.StringUtils;

/**
 * @author hans
 */
public class ProcessorNameParser {

    private String bundleName;
    private String bundleVersion;
    private String executableName;

    public ProcessorNameParser(String processorIdentifier) throws InvalidProcessorIdException {
        parse(processorIdentifier);
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

    private void parse(String processorIdentifier) throws InvalidProcessorIdException {
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
}
