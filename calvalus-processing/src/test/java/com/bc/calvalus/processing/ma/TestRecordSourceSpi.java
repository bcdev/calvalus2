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
        RecordUtils.addPointRecord(recordSource, 0F, 0F);
        RecordUtils.addPointRecord(recordSource, 1F, 1F);
        RecordUtils.addPointRecord(recordSource, 2F, 2F);
        return recordSource;
    }

    @Override
    protected boolean canDecodeContent(String recordSourceUrl) {
        return false;
    }
}
