package com.bc.calvalus.processing.ma;

import org.esa.beam.dataio.placemark.PlacemarkIO;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PinDescriptor;
import org.esa.beam.framework.datamodel.Placemark;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A record source that creates records from BEAM placemark XML.
 *
 * @author MarcoZ
 * @author Norman
 */
public class PlacemarkRecordSource implements RecordSource {
    public static final String CALVALUS_PLACEMARK_RECORD_SOURCE_URI = "calvalus.placemarkRecordSource.uri";
    public static final String[] ATTRIBUTE_NAMES = new String[]{"name", "latitude", "longitude"};

    private final Header header;
    private final Reader reader;

    /**
     * Constructor.
     *
     * @param reader The reader for the placemark XML.
     */
    public PlacemarkRecordSource(Reader reader) {
        this.header = new DefaultHeader(ATTRIBUTE_NAMES);
        this.reader = reader;

    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public Iterable<Record> getRecords() throws Exception {
        List<Placemark> placemarks = PlacemarkIO.readPlacemarks(reader, null, PinDescriptor.getInstance());
        List<Record> records = new ArrayList<Record>(placemarks.size());
        for (Placemark placemark : placemarks) {
            records.add(new PlacemarkRecord(placemark));
        }
        return records;
    }

    public static class Spi extends RecordSourceSpi {

        @Override
        public RecordSource createRecordSource(Map<String, String> config) throws Exception {
            String uriString = config.get(CALVALUS_PLACEMARK_RECORD_SOURCE_URI);
            URI uri = new URI(uriString);
            InputStream inputStream = uri.toURL().openStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            PlacemarkRecordSource placemarkRecordSource = new PlacemarkRecordSource(inputStreamReader);
            return placemarkRecordSource;
        }
    }

    private class PlacemarkRecord implements Record {

        private final Placemark placemark;

        public PlacemarkRecord(Placemark placemark) {
            this.placemark = placemark;
        }

        @Override
        public Object[] getAttributeValues() {
            return new Object[]{
                    placemark.getName(),
                    placemark.getGeoPos().lat,
                    placemark.getGeoPos().lon,
            };
        }

        @Override
        public GeoPos getCoordinate() {
            return placemark.getGeoPos();
        }
    }
}
