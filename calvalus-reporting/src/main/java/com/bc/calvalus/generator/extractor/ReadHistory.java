package com.bc.calvalus.generator.extractor;

/**
 * @author muhammad.bc.
 */
public class ReadHistory extends ReaderHistorySource {
    private final String sourceUrl;
    private final FormatType formatType;

    public ReadHistory(String sourceUrl) {
        this(sourceUrl, FormatType.XML);
    }

    public ReadHistory(String sourceUrl, FormatType formatType) {
        super(sourceUrl);
        this.sourceUrl = sourceUrl;
        this.formatType = formatType;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public FormatType getFormatType() {
        return formatType;
    }

    public String getRawSource() {
        return readSource(formatType);
    }
}
