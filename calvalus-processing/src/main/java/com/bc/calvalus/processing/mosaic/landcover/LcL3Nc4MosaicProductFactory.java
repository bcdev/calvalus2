package com.bc.calvalus.processing.mosaic.landcover;

import com.bc.calvalus.processing.mosaic.MosaicProductFactory;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.ColorPaletteDef;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.ImageInfo;
import org.esa.beam.framework.datamodel.IndexCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

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
class LcL3Nc4MosaicProductFactory implements MosaicProductFactory {

    static final float[] MERIS_WAVELENGTH = new float[]{
            412.691f, 442.55902f, 489.88202f, 509.81903f, 559.69403f,
            619.601f, 664.57306f, 680.82104f, 708.32904f, 753.37103f,
            761.50806f, 778.40906f, 864.87604f, 884.94403f, 900.00006f};

    static final String[] SPOT_BANDNAMES = new String[] { "B0", "B2", "B3", "MIR" };
    static final float[] SPOT_WAVELENGTH = new float[]{450f, 645f, 835f, 1665f};

    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    static final SimpleDateFormat COMPACT_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        COMPACT_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    static String[] NORTHING_TILE_NAME = {
            "90S", "85S", "80S", "75S", "70S", "65S",
            "60S", "55S", "50S", "45S", "40S", "35S",
            "30S", "25S", "20S", "15S", "10S", "05S",
            "00N", "05N", "10N", "15N", "20N", "25N",
            "30N", "35N", "40N", "45N", "50N", "55N",
            "60N", "65N", "70N", "75N", "80N", "85N" };
    static String[] EASTING_TILE_NAME = {
            "180W", "175W", "170W", "165W", "160W", "155W",
            "150W", "145W", "140W", "135W", "130W", "125W",
            "120W", "115W", "110W", "105W", "100W", "095W",
            "090W", "085W", "080W", "075W", "070W", "065W",
            "060W", "055W", "050W", "045W", "040W", "035W",
            "030W", "025W", "020W", "015W", "010W", "005W",
            "000E", "005E", "010E", "015E", "020E", "025E",
            "030E", "035E", "040E", "045E", "050E", "055E",
            "060E", "065E", "070E", "075E", "080E", "085E",
            "090E", "095E", "100E", "105E", "110E", "115E",
            "120E", "125E", "130E", "135E", "140E", "145E",
            "150E", "155E", "160E", "165E", "170E", "175E" };

    private final String[] outputFeatures;
    private final float[] wavelength;

    public LcL3Nc4MosaicProductFactory(String[] outputFeatures) {
        this.outputFeatures = outputFeatures;
        this.wavelength = outputFeatures.length > 30 ? MERIS_WAVELENGTH : SPOT_WAVELENGTH;
    }

    public static String tileName(int tileY, int tileX) {
        return String.format("h%02dv%02d", tileX, tileY);
    }

    public static String neswTileName(int tileY, int tileX) {
        return NORTHING_TILE_NAME[NORTHING_TILE_NAME.length - 1 - tileY] + EASTING_TILE_NAME[tileX];
    }

    @Override
    public Product createProduct(Configuration configuration, int tileX, int tileY, Rectangle rect) {

        String minDateParameter = configuration.get("calvalus.minDate");
        String maxDateParameter = configuration.get("calvalus.maxDate");
        String tileSizeParameter = configuration.get("calvalus.mosaic.tileSize");
        Date minDate;
        Date maxDate;
        try {
            minDate = DATE_FORMAT.parse(minDateParameter);
            maxDate = DATE_FORMAT.parse(maxDateParameter);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        calendar.setTime(maxDate);
        calendar.add(Calendar.DATE, 1);
        Date endDate = calendar.getTime();


        String sensor = configuration.get("calvalus.lc.sensor", "MERIS"); // VEGETATION
        String platform = sensor.equals("MERIS") ? "ENVISAT" : "SPOT";
        String spatialResolution = "360".equals(tileSizeParameter) ? "300" : "1000";
        String temporalResolution = "7";  // TODO
        String startTime = COMPACT_DATE_FORMAT.format(minDate);
        String version = "1.0";
        String productName = MessageFormat.format("ESACCI-LC-L3-SR-{0}-{1}m-P{2}D-{3}-{4}-v{5}",
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
        band.setNoDataValue(-1);
        band.setNoDataValueUsed(true);
        band.setDescription("LC pixel type mask");

        IndexCoding indexCoding = new IndexCoding("current_pixel_state");
        ColorPaletteDef.Point[] points = new ColorPaletteDef.Point[6];
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

        List<String> countBandNames = new ArrayList<String>(outputFeatures.length);
        List<String> srMeanBandNames = new ArrayList<String>(outputFeatures.length);
        List<String> srSigmaBandNames = new ArrayList<String>(outputFeatures.length);
        for (String outputFeature : outputFeatures) {
            if (outputFeature.endsWith("_count")) {
                countBandNames.add(outputFeature);
            } else if (outputFeature.startsWith("sr_")) {
                if (outputFeature.endsWith("_mean")) {
                    srMeanBandNames.add(outputFeature);
                } else if (outputFeature.endsWith("_uncertainty")) {
                    srSigmaBandNames.add(outputFeature);
                }
            }
        }

        for (String counter : countBandNames) {
            band = product.addBand(counter, ProductData.TYPE_INT16);
            band.setNoDataValue(-1);
            band.setNoDataValueUsed(true);
        }

        for (int i = 0; i < srMeanBandNames.size(); i++) {
            final String bandName = srMeanBandNames.get(i);
            int bandIndex = -1;
            try {
                final String inputBandName = bandName.substring("sr_".length(), bandName.length() - "_mean".length());
                for (int j=0; j<SPOT_BANDNAMES.length; ++j) {
                    if (SPOT_BANDNAMES[j].equals(inputBandName)) {
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

    @Override
    public Product createProduct(String productName, Rectangle rect) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public String getTileProductName(String outputNameFormat, int tileX, int tileY) {
        throw new UnsupportedOperationException("not implemented");
    }
}
