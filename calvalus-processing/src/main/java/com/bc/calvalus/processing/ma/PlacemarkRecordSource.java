package com.bc.calvalus.processing.ma;

import org.esa.beam.dataio.placemark.PlacemarkIO;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PinDescriptor;
import org.esa.beam.framework.datamodel.Placemark;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * A record source that creates records from BEAM placemark XML.
 *
 * @author MarcoZ
 * @author Norman
 */
public class PlacemarkRecordSource implements RecordSource {
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
        public RecordSource createRecordSource(MAConfig config) {
            return null;
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
