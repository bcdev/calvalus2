package com.bc.calvalus.processing.ma;

import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

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
    public void testThat78PlacemarksCanBeRead() throws Exception {
        InputStream resourceAsStream = getClass().getResourceAsStream("CEOS_AERONET.placemark");
        Reader reader = new InputStreamReader(resourceAsStream);
        RecordSource recordSource = new PlacemarkRecordSource(reader);
        Iterable<Record> records = recordSource.getRecords();
        assertNotNull(records);
        int counter = 0;
        for (Record record : records) {
            counter++;
        }
        assertEquals(78, counter);
    }
}
