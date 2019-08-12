package com.bc.calvalus.processing.ma;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author MarcoZ
 * @author Norman
 */
public class PlacemarkRecordSourceTest {

    @Test
    public void testThatPlacemarkRecordSourceIsRegisteredAsService() throws Exception {
        RecordSourceSpi spi = RecordSourceSpi.getForClassName("com.bc.calvalus.processing.ma.PlacemarkRecordSource$Spi");
        assertNotNull(spi);
    }

    @Test
    public void testThatPlacemarkSpiCanProduce78Records() throws Exception {
        RecordSourceSpi spi = RecordSourceSpi.getForClassName("com.bc.calvalus.processing.ma.PlacemarkRecordSource$Spi");
        assertNotNull(spi);
        String url = getClass().getResource("CEOS_AERONET.placemark").toURI().toString();
        RecordSource recordSource = spi.createRecordSource(url, new Configuration());
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
