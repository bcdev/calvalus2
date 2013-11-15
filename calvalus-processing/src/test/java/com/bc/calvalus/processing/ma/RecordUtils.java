package com.bc.calvalus.processing.ma;

import org.esa.beam.framework.datamodel.GeoPos;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

/**
 * @author Norman
 */
@Ignore
public class RecordUtils {

    public static Record create(Object... values) {
        return new DefaultRecord(null, null, values);
    }

    public static Record create(GeoPos coordinate, Date time, Object... values) {
        if (coordinate != null || time != null) {
            ArrayList<Object> list;
            if (coordinate != null && time != null) {
                list = new ArrayList<Object>(Arrays.asList((Object) coordinate.lat, coordinate.lon, time));
            } else if (coordinate != null) {
                list = new ArrayList<Object>(Arrays.asList((Object) coordinate.lat, coordinate.lon));
            } else {
                list = new ArrayList<Object>(Arrays.asList((Object) time));
            }
            list.addAll(Arrays.asList(values));
            return new DefaultRecord(coordinate, time, list.toArray(new Object[list.size()]));
        } else {
            return new DefaultRecord(null, null, values);
        }
    }

    public static void addPointRecord(DefaultRecordSource recordSource, float lat, float lon, Object... attributeValues) {
        recordSource.addRecord(create(new GeoPos(lat, lon), null, attributeValues));
    }
}
