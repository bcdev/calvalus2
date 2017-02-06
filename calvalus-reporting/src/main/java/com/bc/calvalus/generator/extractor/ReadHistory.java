package com.bc.calvalus.generator.extractor;

/**
 * @author muhammad.bc.
 */
public class ReadHistory extends ReadHistoryFromSource {
    private final String sourceUrl;
    private final ReadFormatType readFormatType;

    public ReadHistory(String sourceUrl) {
        this(sourceUrl, ReadFormatType.XML);
    }

    public ReadHistory(String sourceUrl, ReadFormatType readFormatType) {
        super(sourceUrl);
        this.sourceUrl = sourceUrl;
        this.readFormatType = readFormatType;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public ReadFormatType getReadFormatType() {
        return readFormatType;
    }

    public String getRawSource() {
        return readSource(readFormatType);
    }
}
