package com.bc.calvalus.processing.ma;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.esa.beam.dataio.placemark.PlacemarkIO;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PinDescriptor;
import org.esa.beam.framework.datamodel.Placemark;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * A record source that creates records from BEAM placemark XML.
 *
 * @author MarcoZ
 * @author Norman
 */
public class PlacemarkRecordSource implements RecordSource {
    public static final String URL_PARAM_NAME = "url";
    public static final String[] ATTRIBUTE_NAMES = new String[]{
            "placemark_name",
            "placemark_latitude",
            "placemark_longitude"};

    private final Header header;
    private final Reader reader;

    /**
     * Constructor.
     *
     * @param reader The reader for the placemark XML.
     */
    public PlacemarkRecordSource(Reader reader) {
        this.header = new DefaultHeader(true, false, ATTRIBUTE_NAMES);
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
        static {
            try {
                URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
            } catch (Throwable e) {
                // ignore as its most likely already set
                String msg = "Cannot set URLStreamHandlerFactory due " + e.getMessage() + ". This exception will be ignored.";
                CalvalusLogger.getLogger().fine(msg);
            }
        }

        @Override
        public RecordSource createRecordSource(String url) throws Exception {
            InputStream inputStream = new URL(url).openStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            return new PlacemarkRecordSource(inputStreamReader);
        }
    }

    private final class PlacemarkRecord implements Record {

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
        public GeoPos getLocation() {
            return placemark.getGeoPos();
        }

        @Override
        public Date getTime() {
            return null;
        }
    }
}
