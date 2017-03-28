package com.bc.calvalus.extractor;

/**
 * @author muhammad.bc.
 */
public class ClientConnectionToHistory extends ClientConnectionToHistoryServer {
    private final String sourceUrl;
    private final ReadFormatType readFormatType;

    public ClientConnectionToHistory(String sourceUrl) {
        this(sourceUrl, ReadFormatType.XML);
    }

    public ClientConnectionToHistory(String sourceUrl, ReadFormatType readFormatType) {
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

    public String getResponse() {
        return readSource(readFormatType);
    }
}
