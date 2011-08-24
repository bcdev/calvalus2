package com.bc.calvalus.processing.ma;

import org.junit.Ignore;

/**
* @author Norman
*/
@Ignore
public class TestRecordSourceSpi extends RecordSourceSpi {
    @Override
    public RecordSource createRecordSource(String url) {
        DefaultHeader header = new DefaultHeader(true, "lat", "lon");
        DefaultRecordSource recordSource = new DefaultRecordSource(header);
        ExtractorTest.addPointRecord(recordSource, 0F, 0F);
        ExtractorTest.addPointRecord(recordSource, 1F, 1F);
        ExtractorTest.addPointRecord(recordSource, 2F, 2F);
        return recordSource;
    }
}
