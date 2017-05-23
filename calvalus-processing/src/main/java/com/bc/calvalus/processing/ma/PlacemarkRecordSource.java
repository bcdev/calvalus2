package com.bc.calvalus.processing.ma;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.placemark.PlacemarkIO;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PinDescriptor;
import org.esa.snap.core.datamodel.Placemark;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * A record source that creates records from SNAP placemark XML.
 *
 * @author MarcoZ
 * @author Norman
 */
public class PlacemarkRecordSource implements RecordSource {

    public static final String URL_PARAM_NAME = "url";
    public static final String[] ATTRIBUTE_NAMES = new String[]{
            "placemark_name",
            "placemark_latitude",
            "placemark_longitude"
    };

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
        int recordId = 0;
        for (Placemark placemark : placemarks) {
            records.add(new PlacemarkRecord(placemark, recordId++));
        }
        return records;
    }

    @Override
    public String getTimeAndLocationColumnDescription() {
        return "SNAP placemark format";
    }

    public static class Spi extends RecordSourceSpi {

        @Override
        public RecordSource createRecordSource(String url) throws Exception {
            InputStream inputStream;
            if (url.startsWith("hdfs:")) {
                final Configuration conf = new Configuration();
                final Path path = new Path(url);
                inputStream = path.getFileSystem(conf).open(path);
            } else {
                inputStream = new URL(url).openStream();
            }
            Reader reader = new BufferedReader(new InputStreamReader(inputStream));
            return new PlacemarkRecordSource(reader);
        }

        @Override
        public String[] getAcceptedExtensions() {
            return new String[]{".placemark"};
        }
    }

    private final class PlacemarkRecord implements Record {

        private final Placemark placemark;
        private final int recordId;

        public PlacemarkRecord(Placemark placemark, int recordId) {
            this.placemark = placemark;
            this.recordId = recordId;
        }

        @Override
        public int getId() {
            return recordId;
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
        public Object[] getAnnotationValues() {
            return new Object[0];
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
