package com.bc.calvalus.plot;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public final class TimeUtils {

    public static final long TIME_NULL = Long.MIN_VALUE;
    private static final SimpleDateFormat CCSDS_UTC_MILLIS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final SimpleDateFormat CCSDS_UTC_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final SimpleDateFormat CCSDS_LOCAL_WITHOUT_T_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    static {
//        CCSDS_UTC_MILLIS_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
//        CCSDS_UTC_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private TimeUtils() {
        // prevent instantiation
    }

    public static String formatCcsdsUtcFormat(long timeInMillis) {
        if (timeInMillis == TIME_NULL) {
            return "";
        }
        return CCSDS_UTC_FORMAT.format(new Date(timeInMillis));
    }

    public static long parseCcsdsLocalTimeWithoutT(String timeString) throws ParseException {
        if (timeString.isEmpty()) {
            return TIME_NULL;
        }
        return CCSDS_LOCAL_WITHOUT_T_FORMAT.parse(timeString).getTime();
    }

    public static long parseCcsdsUtcFormat(String timeString) throws ParseException {
        if (timeString == null || timeString.isEmpty()) {
            return TIME_NULL;
        }
        if (timeString.length() == "yyyy-MM-ddTHH:MM:ssZ".length()) {
            return CCSDS_UTC_FORMAT.parse(timeString).getTime();
        }
        return CCSDS_UTC_MILLIS_FORMAT.parse(timeString).getTime();
    }

    public static Date createDate(int day, int month, int year) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month, day);
        return calendar.getTime();
    }
}
