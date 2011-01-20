package com.bc.calvalus.binning.job;

import com.bc.calvalus.binning.Aggregator;
import com.bc.calvalus.binning.AggregatorAverage;
import com.bc.calvalus.binning.AggregatorAverageML;
import com.bc.calvalus.binning.AggregatorMinMax;
import com.bc.calvalus.binning.AggregatorOnMaxSet;
import com.bc.calvalus.binning.BinManager;
import com.bc.calvalus.binning.BinManagerImpl;
import com.bc.calvalus.binning.BinningContext;
import com.bc.calvalus.binning.BinningContextImpl;
import com.bc.calvalus.binning.BinningGrid;
import com.bc.calvalus.binning.IsinBinningGrid;
import com.bc.calvalus.binning.VariableContext;
import com.bc.calvalus.binning.VariableContextImpl;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.datamodel.GeoCoding;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.util.ProductUtils;

import java.awt.Rectangle;
import java.awt.geom.GeneralPath;
import java.awt.geom.Path2D;
import java.awt.geom.PathIterator;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static java.lang.Math.*;

/**
 * Creates the binning context from a job's configuration.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L3Config {

    private static final String EODATA_PATH = "hdfs://cvmaster00:9000/calvalus/eodata/MER_RR__1P/r03/%04d/%02d/%02d";
    public static final String CONFNAME_L3_NUM_SCANS_PER_SLICE = "calvalus.l3.numScansPerSlice";
    public static final String CONFNAME_L3_GRID_NUM_ROWS = "calvalus.l3.grid.numRows";
    public static final String CONFNAME_L3_START_DATE = "calvalus.l3.startDate";
    public static final String CONFNAME_L3_NUM_DAYS = "calvalus.l3.numDays";
    public static final String CONFNAME_L3_BBOX = "calvalus.l3.bbox";
    public static final String CONFNAME_L3_REGION = "calvalus.l3.region";
    public static final String CONFNAME_L3_AGG_i_TYPE = "calvalus.l3.aggregators.%d.type";
    public static final String CONFNAME_L3_AGG_i_VAR_NAME = "calvalus.l3.aggregators.%d.varName";
    public static final String CONFNAME_L3_AGG_i_VAR_NAMES_j = "calvalus.l3.aggregators.%d.varNames.%d";
    public static final String CONFNAME_L3_AGG_i_WEIGHT_COEFF = "calvalus.l3.aggregators.%d.weightCoeff";
    public static final String CONFNAME_L3_MASK_EXPR = "calvalus.l3.maskExpr";
    public static final String CONFNAME_L3_VARIABLES_i_NAME = "calvalus.l3.variables.%d.name";
    public static final String CONFNAME_L3_VARIABLES_i_EXPR = "calvalus.l3.variables.%d.expr";
    public static final String CONFNAME_L3_OUTPUT = "calvalus.l3.output";
    public static final String CONFNAME_L3_OPERATOR_NAME = "calvalus.l3.operator";
    public static final String CONFNAME_L3_OPERATOR_PARMETER_PREFIX = "calvalus.l3.operator.";
    public static final int DEFAULT_L3_NUM_SCANS_PER_SLICE = 64;

    public static final int DEFAULT_L3_NUM_NUM_DAYS = 16;

    public static final String L3_REQUEST_PROPERTIES_FILENAME = "l3request.properties";
    private final Properties properties;

    public L3Config(Properties properties) {
        this.properties = properties;
    }

    public void copyToConfiguration(Configuration configuration) {
        for (String key : properties.stringPropertyNames()) {
            configuration.set(key, properties.getProperty(key));
        }
    }

    public Product getPreProcessedProduct(Product product) {

        product = getProductSpatialSubset(product);
        if (product == null) {
            return null;
        }

        String operatorName = properties.getProperty(CONFNAME_L3_OPERATOR_NAME);
        if (operatorName == null) {
            return product;
        }

        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        Map<String, Object> parameterMap = new HashMap<String, Object>();
        for (String propertyName : properties.stringPropertyNames()) {
            if (propertyName.startsWith(CONFNAME_L3_OPERATOR_PARMETER_PREFIX)) {
                String paramName = propertyName.substring(CONFNAME_L3_OPERATOR_PARMETER_PREFIX.length());
                String paramValue = properties.getProperty(propertyName);
                parameterMap.put(paramName, paramValue);
            }
        }
        return GPF.createProduct(operatorName, parameterMap, product);
    }

    private Product getProductSpatialSubset(Product product) {
        final Geometry geoRegion = getRegionOfInterest();
        if (geoRegion == null || geoRegion.isEmpty()) {
            return product;
        }

        final Rectangle pixelRegion = computePixelRegion(product, geoRegion);
        if (pixelRegion == null || pixelRegion.isEmpty()) {
            return null;
        }

        final SubsetOp op = new SubsetOp();
        op.setSourceProduct(product);
        op.setRegion(pixelRegion);
        op.setCopyMetadata(false);
        return op.getTargetProduct();
    }

    static Rectangle computePixelRegion(Product product, Geometry geoRegion) {
        return computePixelRegion(product, geoRegion, 1);
    }

    static Rectangle computePixelRegion(Product product, Geometry geoRegion, int numBorderPixels) {
        final Geometry productGeometry = computeProductGeometry(product);
        final Geometry regionIntersection = geoRegion.intersection(productGeometry);
        if (regionIntersection.isEmpty()) {
            return null;
        }
        final PixelRegionFinder pixelRegionFinder = new PixelRegionFinder(product.getGeoCoding());
        regionIntersection.apply(pixelRegionFinder);
        final Rectangle pixelRegion = pixelRegionFinder.getPixelRegion();
        pixelRegion.grow(numBorderPixels, numBorderPixels);
        return pixelRegion.intersection(new Rectangle(product.getSceneRasterWidth(),
                                                      product.getSceneRasterHeight()));
    }

    static Geometry computeProductGeometry(Product product) {
        final GeneralPath[] paths = ProductUtils.createGeoBoundaryPaths(product);
        final Polygon[] polygons = new Polygon[paths.length];
        final GeometryFactory factory = new GeometryFactory();
        for (int i = 0; i < paths.length; i++) {
            polygons[i] = convertAwtPathToJtsPolygon(paths[i], factory);
        }
        final DouglasPeuckerSimplifier peuckerSimplifier = new DouglasPeuckerSimplifier(polygons.length == 1 ? polygons[0] : factory.createMultiPolygon(polygons));
        return peuckerSimplifier.getResultGeometry();
    }

    private static Polygon convertAwtPathToJtsPolygon(Path2D path, GeometryFactory factory) {
        final PathIterator pathIterator = path.getPathIterator(null);
        ArrayList<double[]> coordList = new ArrayList<double[]>();
        int lastOpenIndex = 0;
        while (!pathIterator.isDone()) {
            final double[] coords = new double[6];
            final int segType = pathIterator.currentSegment(coords);
            if (segType == PathIterator.SEG_CLOSE) {
                // we should only detect a single SEG_CLOSE
                coordList.add(coordList.get(lastOpenIndex));
                lastOpenIndex = coordList.size();
            } else {
                coordList.add(coords);
            }
            pathIterator.next();
        }
        final Coordinate[] coordinates = new Coordinate[coordList.size()];
        for (int i1 = 0; i1 < coordinates.length; i1++) {
            final double[] coord = coordList.get(i1);
            coordinates[i1] = new Coordinate(coord[0], coord[1]);
        }

        return factory.createPolygon(factory.createLinearRing(coordinates), null);
    }

    public BinningContext getBinningContext() {
        VariableContext varCtx = getVariableContext();
        return new BinningContextImpl(getBinningGrid(),
                                      varCtx,
                                      getBinManager(varCtx));
    }

    public BinningGrid getBinningGrid() {
        String stringValue = properties.getProperty(CONFNAME_L3_GRID_NUM_ROWS);
        int numRows = 2160;
        if (stringValue != null) {
            numRows = Integer.parseInt(stringValue);
        }
        return new IsinBinningGrid(numRows);
    }

    public BinManager getBinManager() {
        return getBinManager(getVariableContext());
    }

    public String[] getInputPath() {
        final int numDays = getNumDays();
        String[] inputPaths = new String[numDays];
        ProductData.UTC startTime = getStartTime();
        Calendar calendar = startTime.getAsCalendar();
        for (int i = 0; i < inputPaths.length; i++) {
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH) + 1;
            int day = calendar.get(Calendar.DAY_OF_MONTH);
            inputPaths[i] = String.format(EODATA_PATH, year, month, day);
            calendar.add(Calendar.DAY_OF_MONTH, 1);
        }
        return inputPaths;
    }

    private int getNumDays() {
        String numDaysString = properties.getProperty(CONFNAME_L3_NUM_DAYS);
        final int numDays;
        if (numDaysString != null) {
            numDays = Integer.parseInt(numDaysString);
        } else {
            numDays = DEFAULT_L3_NUM_NUM_DAYS;
        }
        return numDays;
    }

    public ProductData.UTC getStartTime() {
        String startDateString = properties.getProperty(CONFNAME_L3_START_DATE);
        if (startDateString == null) {
            throw new IllegalArgumentException(MessageFormat.format("Parameter: ''{0}'' not given.", CONFNAME_L3_START_DATE));
        }
        try {
            return ProductData.UTC.parse(startDateString, "yyyy-MM-dd");
        } catch (ParseException e) {
            throw new IllegalArgumentException("Illegal start date format.", e);
        }
    }

    public ProductData.UTC getEndTime() {
        ProductData.UTC start = getStartTime();
        Calendar calendar = start.getAsCalendar();
        calendar.add(Calendar.DAY_OF_MONTH, getNumDays() - 1);
        return ProductData.UTC.create(calendar.getTime(), 0);
    }

    public Path getOutput() {
        return new Path(properties.getProperty(CONFNAME_L3_OUTPUT));
    }

    public Geometry getRegionOfInterest() {
        String regionWkt = properties.getProperty(CONFNAME_L3_REGION);
        if (regionWkt == null) {
            final String bboxStr = properties.getProperty(CONFNAME_L3_BBOX);
            if (bboxStr == null) {
                return null;
            }
            final String[] coords = bboxStr.split(",");
            if (coords.length != 4) {
                throw new IllegalArgumentException(MessageFormat.format("Illegal BBOX value: {0}", bboxStr));
            }
            String x1 = coords[0];
            String y1 = coords[1];
            String x2 = coords[2];
            String y2 = coords[3];
            regionWkt = String.format("POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))",
                                      x1, y1,
                                      x2, y1,
                                      x2, y2,
                                      x1, y2,
                                      x1, y1);
        }

        final WKTReader wktReader = new WKTReader();
        try {
            return wktReader.read(regionWkt);
        } catch (com.vividsolutions.jts.io.ParseException e) {
            throw new IllegalArgumentException("Illegal region geometry: " + regionWkt, e);
        }
    }

    private BinManager getBinManager(VariableContext varCtx) {
        ArrayList<Aggregator> aggregators = new ArrayList<Aggregator>();
        for (int i = 0; ; i++) {
            String type = properties.getProperty(String.format(CONFNAME_L3_AGG_i_TYPE, i));
            if (type == null) {
                break;
            }
            Aggregator aggregator;
            if (type.equals("AVG")) {
                aggregator = getAggregatorAverage(varCtx, i);
            } else if (type.equals("AVG_ML")) {
                aggregator = getAggregatorAverageML(varCtx, i);
            } else if (type.equals("MIN_MAX")) {
                aggregator = getAggregatorMinMax(varCtx, i);
            } else if (type.equals("ON_MAX_SET")) {
                aggregator = getAggregatorOnMaxSet(varCtx, i);
            } else {
                throw new IllegalArgumentException("Unknown aggregator type: " + type);
            }
            aggregators.add(aggregator);
        }
        return new BinManagerImpl(aggregators.toArray(new Aggregator[aggregators.size()]));
    }

    public VariableContext getVariableContext() {
        VariableContextImpl variableContext = new VariableContextImpl();

        variableContext.setMaskExpr(properties.getProperty(CONFNAME_L3_MASK_EXPR));

        // define declared variables
        //
        for (int i = 0; ; i++) {
            String varName = properties.getProperty(String.format(CONFNAME_L3_VARIABLES_i_NAME, i));
            String varExpr = properties.getProperty(String.format(CONFNAME_L3_VARIABLES_i_EXPR, i));
            if (varName == null) {
                break;
            }
            variableContext.defineVariable(varName, varExpr);
        }

        // define variables of all aggregators
        //
        for (int i = 0; ; i++) {
            String aggType = properties.getProperty(String.format(CONFNAME_L3_AGG_i_TYPE, i));
            if (aggType == null) {
                break;
            }
            String varName = properties.getProperty(String.format(CONFNAME_L3_AGG_i_VAR_NAME, i));
            if (varName != null) {
                variableContext.defineVariable(varName);
            } else {
                for (int j = 0; ; j++) {
                    varName = properties.getProperty(String.format(CONFNAME_L3_AGG_i_VAR_NAMES_j, i, j));
                    if (varName != null) {
                        variableContext.defineVariable(varName);
                    } else {
                        break;
                    }
                }
            }
        }

        return variableContext;
    }

    private Aggregator getAggregatorAverage(VariableContext varCtx, int i) {
        String varName = properties.getProperty(String.format(CONFNAME_L3_AGG_i_VAR_NAME, i));
        String weightCoeff = properties.getProperty(String.format(CONFNAME_L3_AGG_i_WEIGHT_COEFF, i));
        if (weightCoeff == null) {
            return new AggregatorAverage(varCtx, varName);
        } else {
            return new AggregatorAverage(varCtx, varName, Double.parseDouble(weightCoeff));
        }
    }

    private Aggregator getAggregatorAverageML(VariableContext varCtx, int i) {
        String varName = properties.getProperty(String.format(CONFNAME_L3_AGG_i_VAR_NAME, i));
        String weightCoeff = properties.getProperty(String.format(CONFNAME_L3_AGG_i_WEIGHT_COEFF, i));
        if (weightCoeff == null) {
            return new AggregatorAverageML(varCtx, varName);
        } else {
            return new AggregatorAverageML(varCtx, varName, Double.parseDouble(weightCoeff));
        }
    }

    private Aggregator getAggregatorMinMax(VariableContext varCtx, int i) {
        String varName = properties.getProperty(String.format(CONFNAME_L3_AGG_i_VAR_NAME, i));
        return new AggregatorMinMax(varCtx, varName);
    }

    private Aggregator getAggregatorOnMaxSet(VariableContext varCtx, int i) {
        ArrayList<String> varNames = new ArrayList<String>();
        for (int j = 0; ; j++) {
            String varName = properties.getProperty(String.format(CONFNAME_L3_AGG_i_VAR_NAMES_j, i, j));
            if (varName == null) {
                break;
            }
            varNames.add(varName);
        }
        return new AggregatorOnMaxSet(varCtx, varNames.toArray(new String[varNames.size()]));
    }

    static L3Config createFromJobConfig(Configuration conf) {
        Iterator<Map.Entry<String, String>> entryIterator = conf.iterator();
        Properties properties = new Properties();
        while (entryIterator.hasNext()) {
            Map.Entry<String, String> configEntry = entryIterator.next();
            String name = configEntry.getKey();
            if (name.startsWith("calvalus.l3.")) {
                properties.put(name, configEntry.getValue());
            }
        }
        return new L3Config(properties);
    }

    static L3Config load(File file) throws IOException {
        return new L3Config(readProperties(new FileReader(file)));
    }

    public static L3Config load(FileSystem fs, Path file) throws IOException {
        InputStream inputStream = fs.open(file);
        return new L3Config(readProperties(new InputStreamReader(inputStream)));
    }

    public void store(FileSystem fs, Path file) throws IOException {
        FSDataOutputStream os = fs.create(file);
        try {
            properties.store(os, "");
        } finally {
            os.close();
        }
    }

    /**
     * Reads properties and closes the reader.
     *
     * @param reader The reader
     * @return properties
     * @throws IOException on    I/O error
     */
    static Properties readProperties(Reader reader) throws IOException {
        try {
            Properties properties = new Properties();
            properties.load(reader);
            return properties;
        } finally {
            reader.close();
        }
    }

    private static class PixelRegionFinder implements CoordinateFilter {
        private final GeoCoding geoCoding;
        private int x1;
        private int y1;
        private int x2;
        private int y2;

        public PixelRegionFinder(GeoCoding geoCoding) {
            this.geoCoding = geoCoding;
            x1 = Integer.MAX_VALUE;
            x2 = Integer.MIN_VALUE;
            y1 = Integer.MAX_VALUE;
            y2 = Integer.MIN_VALUE;
        }

        @Override
        public void filter(Coordinate coordinate) {
            final GeoPos geoPos = new GeoPos((float) coordinate.y, (float) coordinate.x);
            final PixelPos pixelPos = geoCoding.getPixelPos(geoPos, null);
            if (pixelPos.isValid()) {
                x1 = min(x1, (int) floor(pixelPos.x));
                x2 = max(x2, (int) ceil(pixelPos.x));
                y1 = min(y1, (int) floor(pixelPos.y));
                y2 = max(y2, (int) ceil(pixelPos.y));
            }
        }

        public Rectangle getPixelRegion() {
            return new Rectangle(x1, y1, x2 - x1 + 1, y2 - y1 + 1);
        }
    }
}
