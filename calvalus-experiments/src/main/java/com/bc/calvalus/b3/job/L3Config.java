package com.bc.calvalus.b3.job;

import com.bc.calvalus.b3.Aggregator;
import com.bc.calvalus.b3.AggregatorAverage;
import com.bc.calvalus.b3.AggregatorAverageML;
import com.bc.calvalus.b3.AggregatorMinMax;
import com.bc.calvalus.b3.AggregatorOnMaxSet;
import com.bc.calvalus.b3.BinManager;
import com.bc.calvalus.b3.BinManagerImpl;
import com.bc.calvalus.b3.BinningContext;
import com.bc.calvalus.b3.BinningContextImpl;
import com.bc.calvalus.b3.BinningGrid;
import com.bc.calvalus.b3.IsinBinningGrid;
import com.bc.calvalus.b3.VariableContext;
import com.bc.calvalus.b3.VariableContextImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Creates the binning context from a job's configuration.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L3Config {

    public static final String CONFNAME_L3_NUM_SCANS_PER_SLICE = "calvalus.l3.numScansPerSlice";
    public static final String CONFNAME_L3_GRID_NUM_ROWS = "calvalus.l3.grid.numRows";
    public static final String CONFNAME_L3_NUM_DAYS = "calvalus.l3.numDays";
    public static final String CONFNAME_L3_START_DAY = "calvalus.l3.startDay";
    public static final String CONFNAME_L3_BBOX = "calvalus.l3.bbox";
    public static final String CONFNAME_L3_AGG_i_TYPE = "calvalus.l3.aggregators.%d.type";
    public static final String CONFNAME_L3_AGG_i_VAR_NAME = "calvalus.l3.aggregators.%d.varName";
    public static final String CONFNAME_L3_AGG_i_VAR_NAMES_j = "calvalus.l3.aggregators.%d.varNames.%d";
    public static final String CONFNAME_L3_AGG_i_WEIGHT_COEFF = "calvalus.l3.aggregators.%d.weightCoeff";
    public static final String CONFNAME_L3_MASK_EXPR = "calvalus.l3.maskExpr";
    public static final String CONFNAME_L3_VARIABLES_i_NAME = "calvalus.l3.variables.%d.name";
    public static final String CONFNAME_L3_VARIABLES_i_EXPR = "calvalus.l3.variables.%d.expr";
    public static final String CONFNAME_L3_INPUT = "calvalus.l3.input";
    public static final String CONFNAME_L3_OUTPUT = "calvalus.l3.output";
    public static final String CONFNAME_L3_OPERATOR_NAME = "calvalus.l3.operator";
    public static final String CONFNAME_L3_OPERATOR_PARMETER_PREFIX = "calvalus.l3.operator.";
    public static final int DEFAULT_L3_NUM_SCANS_PER_SLICE = 64;
    public static final int DEFAULT_L3_NUM_NUM_DAYS = 16;

    private static final String L3_REQUEST_PROPERTIES_FILENAME = "l3request.properties";

    private final Properties properties;

    public L3Config(Properties properties) {
        this.properties = properties;
    }

    public void copyToConfiguration(Configuration configuration) {
        for (String key : properties.stringPropertyNames()) {
            configuration.set(key, properties.getProperty(key));
        }
    }

    public Product getProcessedProduct(Product product) {
        final Rectangle2D bbox = getBBox();
        if (bbox != null) {
            // todo - compute firstLine / lastLine
            // todo - use either ProductSubsetBuilder / SubsetOp
        }
        String operatorName = properties.getProperty(CONFNAME_L3_OPERATOR_NAME);
        if (operatorName == null) {
            return product;
        } else {
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

    public Path getInput() {
        return new Path(properties.getProperty(CONFNAME_L3_INPUT));
    }

    public Path getOutput() {
        return new Path(properties.getProperty(CONFNAME_L3_OUTPUT));
    }

    /**
     * @return The bounding box, null refers to BBOX=-180.0,-90.0,180.0,90.0
     */
    public Rectangle2D getBBox() {
        final String bboxStr = properties.getProperty(CONFNAME_L3_BBOX);
        if (bboxStr == null) {
            return null;
        }
        final String[] strings = bboxStr.split(",");
        if (strings.length != 4) {
            throw new IllegalArgumentException(MessageFormat.format("Illegal BBOX value: {0}", bboxStr));
        }
        final double lonMin = Double.parseDouble(strings[0]);
        final double latMin = Double.parseDouble(strings[1]);
        final double lonMax = Double.parseDouble(strings[2]);
        final double latMax = Double.parseDouble(strings[3]);
        final Rectangle2D.Double bbox = new Rectangle2D.Double(lonMin, latMin, lonMax - lonMin, latMax - latMin);
        final double EPS = 1e-10;
        final Rectangle2D.Double GLOBE = new Rectangle2D.Double(-180 + EPS, -90 + EPS, 360 - 2 * EPS, 180 - 2 * EPS);
        if (bbox.contains(GLOBE)) {
            return null;
        }
        return bbox;
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

    static L3Config load(File file) throws IOException {
        return new L3Config(readProperties(new FileReader(file)));
    }

    static L3Config create(Configuration conf) {
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

    static L3Config load(Configuration conf, Path output) throws IOException {
        FileSystem fs = output.getFileSystem(conf);
        InputStream inputStream = fs.open(new Path(output, L3_REQUEST_PROPERTIES_FILENAME));
        return new L3Config(readProperties(new InputStreamReader(inputStream)));
    }

    static Properties readProperties(Reader reader) throws IOException {
        try {
            Properties properties = new Properties();
            properties.load(reader);
            return properties;
        } finally {
            reader.close();
        }
    }

    void writeProperties(Configuration conf, Path output) throws IOException {
        FileSystem fs = output.getFileSystem(conf);
        FSDataOutputStream os = fs.create(new Path(output, L3_REQUEST_PROPERTIES_FILENAME));
        try {
            properties.store(os, "");
        } finally {
            os.close();
        }
    }
}
