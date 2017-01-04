package com.bc.calvalus.processing.mosaic.landcover;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.mosaic.MosaicProductFactory;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.ColorPaletteDef;
import org.esa.snap.core.datamodel.ImageInfo;
import org.esa.snap.core.datamodel.IndexCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.awt.Color;
import java.awt.Rectangle;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

/**
 * The factory for creating the final mosaic product for LC-CCI
 *
 * @author MarcoZ
 */
public class LcL3Nc4MosaicProductFactory implements MosaicProductFactory {

    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    static final SimpleDateFormat COMPACT_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        COMPACT_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final String[] outputFeatures;
    private LcL3SensorConfig sensorConfig = null;

    public LcL3Nc4MosaicProductFactory(String[] outputFeatures) {
        this.outputFeatures = outputFeatures;
    }

    public static String tileName(int tileY, int tileX) {
        return String.format("h%02dv%02d", tileX, tileY);
    }

    @Override
    public Product createProduct(Configuration configuration, int tileX, int tileY, Rectangle rect) {

        if (sensorConfig == null) {
            sensorConfig = LcL3SensorConfig.create(configuration.get("calvalus.lc.resolution"));
        }

        String minDateParameter = configuration.get("calvalus.minDate");
        String maxDateParameter = configuration.get("calvalus.maxDate");
        final Date minDate;
        final Date maxDate;
        try {
            minDate = DATE_FORMAT.parse(minDateParameter);
            maxDate = DATE_FORMAT.parse(maxDateParameter);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        calendar.setTime(maxDate);
        calendar.add(Calendar.DATE, 1);
        final Date endDate = calendar.getTime();

        final String sensor = sensorConfig.getSensorName();
        final String platform = sensorConfig.getPlatformName();
        final String spatialResolution = sensorConfig.getGroundResolution();
        final String temporalResolution = String.format("P%dD", sensorConfig.getPeriod());
        final String startTime = COMPACT_DATE_FORMAT.format(minDate);
        final String version = configuration.get(JobConfigNames.CALVALUS_LC_VERSION, "2.0");
        final float[] wavelength = sensorConfig.getWavelengths();
        final String productName = MessageFormat.format("ESACCI-LC-L3-SR-{0}-{1}-{2}-{3}-{4}-v{5}",
                                                  sensor, spatialResolution, temporalResolution,
                                                  tileName(tileY, tileX), startTime,
                                                  version);

        final Product product = new Product(productName, "CALVALUS-Mosaic", rect.width, rect.height);
//        try {
//            product.setGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, rect.width, rect.height, -180.0+10.0*tileX, 90.0-10.0*tileY, 10.0/rect.width, 10.0/rect.height, 0.0, 0.0));
//        } catch (Exception ex) {
//            throw new RuntimeException("failed to create geocoding for tile", ex);
//        }

        Band band = product.addBand("current_pixel_state", ProductData.TYPE_INT8);
        band.setNoDataValue(0);
        band.setNoDataValueUsed(true);
        band.setDescription("LC pixel type mask");

        final IndexCoding indexCoding = new IndexCoding("current_pixel_state");
        final ColorPaletteDef.Point[] points = new ColorPaletteDef.Point[6];
        indexCoding.addIndex("invalid", 0, "");
        points[0] = new ColorPaletteDef.Point(0, Color.BLACK, "invalid");
        indexCoding.addIndex("clear_land", 1, "");
        points[1] = new ColorPaletteDef.Point(1, Color.GREEN, "land");
        indexCoding.addIndex("clear_water", 2, "");
        points[2] = new ColorPaletteDef.Point(2, Color.BLUE, "water");
        indexCoding.addIndex("clear_snow_ice", 3, "");
        points[3] = new ColorPaletteDef.Point(3, Color.YELLOW, "snow");
        indexCoding.addIndex("cloud", 4, "");
        points[4] = new ColorPaletteDef.Point(4, Color.WHITE, "cloud");
        indexCoding.addIndex("cloud_shadow", 5, "");
        points[5] = new ColorPaletteDef.Point(5, Color.GRAY, "cloud_shadow");
        product.getIndexCodingGroup().add(indexCoding);
        band.setSampleCoding(indexCoding);
        band.setImageInfo(new ImageInfo(new ColorPaletteDef(points, points.length)));

        final List<String> countBandNames = getCountBandNames(outputFeatures);
        final List<String> srMeanBandNames = sensorConfig.getMeanBandNames();
        final List<String> srSigmaBandNames = sensorConfig.getUncertaintyBandNames();

        for (String counter : countBandNames) {
            band = product.addBand(counter, ProductData.TYPE_INT16);
            band.setNoDataValue(-1);
            band.setNoDataValueUsed(true);
        }

        for (int i = 0; i < srMeanBandNames.size(); i++) {
            final String bandName = srMeanBandNames.get(i);
            int bandIndex = sensorConfig.getL2BandIndex(bandName);
/*
            bandIndex = -1;
            try {
                final String inputBandName = bandName.substring("sr_".length(), bandName.length() - "_mean".length());
                for (int j=0; j<sensorConfig.getBandNames().length; ++j) {
                    if (sensorConfig.getBandNames()[j].equals(inputBandName)) {
                        bandIndex = j+1;
                        break;
                    }
                }
                if (bandIndex == -1) {
                    bandIndex = Integer.parseInt(inputBandName);
                }
            } catch (NumberFormatException ex) {
                throw new RuntimeException("cannot determine band index from band name " + bandName, ex);
            }
*/
            band = product.addBand(bandName, ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);
            band.setSpectralBandIndex(bandIndex);
            band.setSpectralWavelength(wavelength[bandIndex-1]);
        }
        band = product.addBand("vegetation_index_mean", ProductData.TYPE_FLOAT32);
        band.setNoDataValue(Float.NaN);
        band.setNoDataValueUsed(true);
        for (String srSigmaBandName : srSigmaBandNames) {
            band = product.addBand(srSigmaBandName, ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);
        }
        product.setAutoGrouping("mean:uncertainty:count");

        product.setStartTime(ProductData.UTC.create(minDate, 0));
        product.setEndTime(ProductData.UTC.create(endDate, 0));
        product.getMetadataRoot().setAttributeString("sensor", sensor);
        product.getMetadataRoot().setAttributeString("platform", platform);
        product.getMetadataRoot().setAttributeString("spatialResolution", spatialResolution);
        product.getMetadataRoot().setAttributeString("temporalResolution", temporalResolution);
        product.getMetadataRoot().setAttributeString("version", version);
        product.getMetadataRoot().setAttributeInt("tileY", tileY);
        product.getMetadataRoot().setAttributeInt("tileX", tileX);

        System.out.println("number of bands: " + product.getBands().length);

        return product;
    }

    private static List<String> getCountBandNames(String[] outputFeatures) {
        final List<String> countBandNames = new ArrayList<String>(outputFeatures.length);
        for (String outputFeature : outputFeatures) {
            if (outputFeature.endsWith("_count")) {
                countBandNames.add(outputFeature);
            }
        }
        return countBandNames;
    }

    @Override
    public Product createProduct(String productName, Rectangle rect) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public String getTileProductName(String outputNameFormat, int tileX, int tileY) {
        throw new UnsupportedOperationException("not implemented");
    }
}
