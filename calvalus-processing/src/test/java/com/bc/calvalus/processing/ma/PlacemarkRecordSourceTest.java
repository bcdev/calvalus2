package com.bc.calvalus.processing.ma;

import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * @author MarcoZ
 * @author Norman
 */
public class PlacemarkRecordSourceTest {

    @Test
    public void testThatPlacemarkRecordSourceIsRegisteredAsService() throws Exception {
        RecordSourceSpi spi = RecordSourceSpi.get("com.bc.calvalus.processing.ma.PlacemarkRecordSource$Spi");
        assertNotNull(spi);
    }

    @Test
    public void testThatPlacemarkSpiCanProduce78Records() throws Exception {
        RecordSourceSpi spi = RecordSourceSpi.get("com.bc.calvalus.processing.ma.PlacemarkRecordSource$Spi");
        assertNotNull(spi);
        HashMap<String, String> config = new HashMap<String, String>();
        config.put(PlacemarkRecordSource.CALVALUS_PLACEMARK_RECORD_SOURCE_URI, getClass().getResource("CEOS_AERONET.placemark").toURI().toString());
        RecordSource recordSource = spi.createRecordSource(config);
        assertNotNull(recordSource);
        assert78(recordSource);
    }

    @Test
    public void testThat78PlacemarksCanBeRead() throws Exception {
        InputStream resourceAsStream = getClass().getResourceAsStream("CEOS_AERONET.placemark");
        Reader reader = new InputStreamReader(resourceAsStream);
        RecordSource recordSource = new PlacemarkRecordSource(reader);
        assert78(recordSource);
    }

    private void assert78(RecordSource recordSource) throws Exception {
        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);
        int counter = 0;
        for (Record record : records) {
            counter++;
        }
        assertEquals(78, counter);
    }
}
