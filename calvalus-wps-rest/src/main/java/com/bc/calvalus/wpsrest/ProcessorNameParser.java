package com.bc.calvalus.wpsrest;

import org.apache.commons.lang.StringUtils;

/**
 * Created by hans on 14/08/2015.
 */
public class ProcessorNameParser {

    private String bundleName;
    private String bundleVersion;
    private String executableName;

    public ProcessorNameParser(String processorIdentifier) throws WpsException {
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

    private void parse(String processorIdentifier) throws WpsException {
        if (!StringUtils.isBlank(processorIdentifier)) {
            String parsedString[] = processorIdentifier.split(Processor.DELIMITER);
            if (parsedString.length < 3) {
                throw new WpsException("Invalid processor identifier in the request URL.");
            }
            this.bundleName = parsedString[0];
            this.bundleVersion = parsedString[1];
            this.executableName = parsedString[2];
        } else {
            throw new WpsException("Invalid processor identifier in the request URL.");
        }
    }
}
