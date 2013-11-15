package com.bc.calvalus.processing.ma;

import org.junit.Ignore;

/**
 * @author Norman
 */
@Ignore
public class TestRecordSourceSpi extends RecordSourceSpi {

    @Override
    public RecordSource createRecordSource(String url) {
        Header header = new TestHeader(true, "lat", "lon");
        DefaultRecordSource recordSource = new DefaultRecordSource(header);
        RecordUtils.addPointRecord(recordSource, 0F, 0F);
        RecordUtils.addPointRecord(recordSource, 1F, 1F);
        RecordUtils.addPointRecord(recordSource, 2F, 2F);
        return recordSource;
    }

    @Override
    public String[] getAcceptedExtensions() {
        return new String[0];
    }
}
