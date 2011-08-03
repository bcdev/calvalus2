package com.bc.calvalus.processing.ma;

import org.esa.beam.dataio.placemark.PlacemarkIO;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PinDescriptor;
import org.esa.beam.framework.datamodel.Placemark;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author MarcoZ
 * @author Norman
 */
public class PlacemarkRecordSource implements RecordSource {
    private Reader reader;

    public PlacemarkRecordSource(Reader reader) {
        this.reader = reader;
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

    public static class Spi extends  RecordSourceSpi {

        @Override
        public RecordSource createRecordSource(MAConfig maConfig) {
            return null;
        }
    }

    private static class PlacemarkRecord implements Record {


        private final Placemark placemark;

        public PlacemarkRecord(Placemark placemark) {
            this.placemark = placemark;
        }

        @Override
        public Header getHeader() {
            return new Header() {
                @Override
                public String[] getAttributeNames() {
                    return new String[] {"name", "latitude", "longitude"};
                }
            };
        }

        @Override
        public Object[] getValues() {
            return new Object[] {
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
