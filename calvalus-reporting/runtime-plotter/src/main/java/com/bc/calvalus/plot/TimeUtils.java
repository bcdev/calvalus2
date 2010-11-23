package com.bc.calvalus.plot;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public final class TimeUtils {
    public static final long TIME_NULL = Long.MIN_VALUE;
    public static final SimpleDateFormat CCSDS_UTC_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    public static final SimpleDateFormat CCSDS_LOCAL_WITHOUT_T_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

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
        return CCSDS_LOCAL_WITHOUT_T_FORMAT.parse(timeString).getTime();
    }

    public static Date createDate(int day, int month, int year) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month, day);
        return calendar.getTime();
    }
}
