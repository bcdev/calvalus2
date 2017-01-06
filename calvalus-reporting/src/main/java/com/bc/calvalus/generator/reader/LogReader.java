package com.bc.calvalus.generator.reader;

/**
 * @author muhammad.bc.
 */
public class LogReader extends SourceReader {
    private String sourceUrl;
    private final FormatType formatType;

    public LogReader(String sourceUrl) {
        this(sourceUrl, FormatType.XML);
    }

    public LogReader(String sourceUrl, FormatType formatType) {
        super(sourceUrl);
        this.sourceUrl = sourceUrl;
        this.formatType = formatType;
    }


    public String getRawSource() {
        return readSource(formatType);
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public FormatType getFormatType() {
        return formatType;
    }
}
