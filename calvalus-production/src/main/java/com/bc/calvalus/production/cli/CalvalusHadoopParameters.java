package com.bc.calvalus.production.cli;

import com.bc.calvalus.processing.ra.RAConfig;
import com.bc.calvalus.processing.ra.RARegions;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

public class CalvalusHadoopParameters extends Configuration {

    public CalvalusHadoopParameters() {
        super(false);
    }

    /**
     * Function for use in production type translation rules
     */
    public String seconds2Millis(String seconds) {
        return seconds + "000";
    }

    /**
     * Function for use in production type translation rules
     */
    public String javaOptsOfMem(String mem) {
        return "-Djava.awt.headless=true -Xmx" + mem + "M";
    }

    /**
     * Function for use in production type translation rules
     */
    public String javaOptsForExec(String mem) {
        return "-Djava.awt.headless=true -Xmx384M";
    }

    /**
     * Function for use in production type translation rules
     */
    public String add512(String mem) {
        return String.valueOf(Integer.parseInt(mem) + 512);
    }

    /**
     * Function for use in production type translation rules
     */
    public String minDateOf(String dateRanges) {
        if (dateRanges == null) { return null; }
        return dateRanges.substring(1, dateRanges.length() - 1).split(":")[0].trim();
    }

    /**
     * Function for use in production type translation rules
     */
    public String maxDateOf(String dateRanges) {
        if (dateRanges == null) { return null; }
        return dateRanges.substring(1, dateRanges.length() - 1).split(":")[1].trim();
    }

    /**
     * Function for use in production type translation rules
     */
    public String minMaxDateRange(String minDate) {
        String dateRanges = get("calvalus.input.dateRanges");
        if (dateRanges != null) {
            return String.format("[%s:%s", minDate, dateRanges.split(":")[1].trim());
        } else {
            return String.format("[%s:]", minDate);
        }
    }

    /**
     * Function for use in production type translation rules
     */
    public String maxMinDateRange(String maxDate) {
        String dateRanges = get("calvalus.input.dateRanges");
        if (dateRanges != null) {
            return String.format("%s:%s]", dateRanges.split(":")[0].trim(), maxDate);
        } else {
            return String.format("[:%s]", maxDate);
        }
    }

    /**
     * Function for use in production type translation rules
     */
    public String listDateRange(String date) {
        return String.format("[%s:%s]", date, date);
    }

    /**
     * Function for use in production type translation rules
     */
    public String periodMinMaxDateRange(String period) {
        String dateRanges = get("calvalus.input.dateRanges");
        String min = minDateOf(dateRanges);
        String max = maxDateOf(dateRanges);
        if (min != null && min.length() > 0 && max != null && max.length() > 0) {
            String compositingPeriod = get("compositingPeriod");
            if (compositingPeriod == null) {
                compositingPeriod = period;
            }
            return computePeriodialDateRanges(period, compositingPeriod, min, max);
        } else {
            set("period", period);
            return dateRanges;
        }
    }

    public String minMaxPeriodDateRange(String min) {
        String dateRanges = get("calvalus.input.dateRanges");
        String max = maxDateOf(dateRanges);
        String period = get("period");
        if (period != null && max != null && max.length() > 0) {
            String compositingPeriod = get("compositingPeriod");
            if (compositingPeriod == null) {
                compositingPeriod = period;
            }
            return computePeriodialDateRanges(period, compositingPeriod, min, max);
        } else {
            return minMaxDateRange(min);
        }
    }

    public String maxMinPeriodDateRange(String max) {
        String dateRanges = get("calvalus.input.dateRanges");
        String period = get("period");
        String min = minDateOf(dateRanges);
        if (period != null && min != null && min.length() > 0) {
            String compositingPeriod = get("compositingPeriod");
            if (compositingPeriod == null) {
                compositingPeriod = period;
            }
            return computePeriodialDateRanges(period, compositingPeriod, min, max);
        } else {
            return maxMinDateRange(max);
        }
    }



    /**
     * Function for use in production type translation rules
     */
    public String compositingPeriodDateRange(String compositingPeriod) {
        String dateRanges = get("calvalus.input.dateRanges");
        String min = minDateOf(dateRanges);
        String max = maxDateOf(dateRanges);
        String period = get("period");
        if (min != null && min.length() > 0 && max != null && max.length() > 0 && period != null) {
            min = dateRanges.substring(1,11);
            max = dateRanges.substring(dateRanges.length()-22, dateRanges.length()-12);
            return computePeriodialDateRanges(period, compositingPeriod, min, max);
        } else {
            set("compositingPeriod", compositingPeriod);
            return dateRanges;
        }
    }

    private String computePeriodialDateRanges(String period, String compositingPeriod, String min, String max) {
        StringBuilder accu = new StringBuilder();
        Calendar cursor = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        cursor.setTime(dateOf(min));
        Calendar end = new GregorianCalendar();
        end.setTime(dateOf(max));
        int periodDays = Integer.parseInt(period);
        int compositingDays = Integer.parseInt(compositingPeriod);
        while (! cursor.after(end)) {
            if (accu.length() > 0) {
                accu.append(",");
            }
            accu.append('[');
            accu.append(dateStringOf(cursor.getTime()));
            accu.append(":");
            cursor.add(Calendar.DATE, compositingDays - 1);
            accu.append(dateStringOf(cursor.getTime()));
            accu.append("]");
            cursor.add(Calendar.DATE, periodDays - compositingDays + 1);
        }
        return accu.toString();
    }

    /**
     * Function for use in production type translation rules.
     * "<parameters> <regionSource>/calvalus/home/martin/region_data/BH.zip</regionSource> <regionSourceAttributeName>HID</regionSourceAttributeName> <goodPixelExpression>pixel_classif_flags.IDEPIX_CLOUD == 0 and pixel_classif_flags.IDEPIX_CLOUD_AMBIGUOUS == 0 and pixel_classif_flags.IDEPIX_CLOUD_BUFFER == 0 and pixel_classif_flags.IDEPIX_CLOUD_SHADOW == 0 and pixel_classif_flags.IDEPIX_SNOW_ICE == 0 and floating_vegetation == 0 and conc_chl &gt; 0.01</goodPixelExpression> <bands> <band> <name>conc_chl</name> <numBins>100</numBins> <min>0</min> <max>100</max> </band> <band> <name>iop_agelb</name> <numBins>15</numBins> <min>0</min> <max>15</max> </band> <band> <name>c2rcc_secchi_depth_3</name> <numBins>15</numBins> <min>0</min> <max>15</max> </band> </bands> <percentiles>90</percentiles> <writePixelValues>true</writePixelValues> <writePerRegion>true</writePerRegion> <writeSeparateHistogram>true</writeSeparateHistogram> </parameters>"
     */
    public String raParameters2Region(String raParameters) {
        try {
            List<Geometry> geometries = new ArrayList<>();
            RAConfig raConfig = getRaConfig(raParameters, geometries);
            Geometry union = CascadedPolygonUnion.union(geometries);
            if (union == null) {
                throw new IllegalArgumentException("Can not build union from given regions");
            }
            Geometry convexHull = union.convexHull();
            return convexHull.toString();
        } catch (BindingException ex) {
            throw new IllegalArgumentException(ex);
        } catch (IOException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Function for use in production type translation rules.
     * "<parameters> <regionSource>/calvalus/home/martin/region_data/BH.zip</regionSource> <regionSourceAttributeName>HID</regionSourceAttributeName> <goodPixelExpression>pixel_classif_flags.IDEPIX_CLOUD == 0 and pixel_classif_flags.IDEPIX_CLOUD_AMBIGUOUS == 0 and pixel_classif_flags.IDEPIX_CLOUD_BUFFER == 0 and pixel_classif_flags.IDEPIX_CLOUD_SHADOW == 0 and pixel_classif_flags.IDEPIX_SNOW_ICE == 0 and floating_vegetation == 0 and conc_chl &gt; 0.01</goodPixelExpression> <bands> <band> <name>conc_chl</name> <numBins>100</numBins> <min>0</min> <max>100</max> </band> <band> <name>iop_agelb</name> <numBins>15</numBins> <min>0</min> <max>15</max> </band> <band> <name>c2rcc_secchi_depth_3</name> <numBins>15</numBins> <min>0</min> <max>15</max> </band> </bands> <percentiles>90</percentiles> <writePixelValues>true</writePixelValues> <writePerRegion>true</writePerRegion> <writeSeparateHistogram>true</writeSeparateHistogram> </parameters>"
     */
    public String raParameters2RaParameters(String raParameters) {
        try {
            RAConfig raConfig = getRaConfig(raParameters, new ArrayList<Geometry>());
            return raConfig.toXml();
        } catch (BindingException ex) {
            throw new IllegalArgumentException(ex);
        } catch (IOException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Function for use in production type translation rules.
     */
    public String tableInputFormat(String table) {
        return "com.bc.calvalus.processing.hadoop.TableInputFormat";
    }

    private RAConfig getRaConfig(String raParameters, List<Geometry> geometries) throws BindingException, IOException {
        RAConfig raConfig = RAConfig.fromXml(raParameters);
        RARegions.RegionIterator regionsIterator = null;
        regionsIterator = raConfig.createNamedRegionIterator(this);
        List<String> names = new ArrayList<>();
        boolean withEnvelope = raConfig.withRegionEnvelope();
        while (regionsIterator.hasNext()) {
            RAConfig.NamedRegion namedRegion = regionsIterator.next();
            Geometry geometry = namedRegion.region;
            if (withEnvelope && geometry.getNumPoints() > 20) {
                geometry = geometry.getEnvelope();
            }
            geometries.add(geometry);
            names.add(namedRegion.name);
        }
        regionsIterator.close();
        if (names.isEmpty()) {
            throw new IllegalArgumentException("No region defined");
        }
        raConfig.setInternalRegionNames(names.toArray(new String[0]));
        return raConfig;
    }


    private static final SimpleDateFormat ISO_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    static {
        ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private String dateStringOf(Date date) {
        return ISO_DATE_FORMAT.format(date);
    }

    private Date dateOf(String date) {
        try {
            return ISO_DATE_FORMAT.parse(date);
        } catch (ParseException e) {
            throw new RuntimeException("failed to parse " + date, e);
        }
    }
}