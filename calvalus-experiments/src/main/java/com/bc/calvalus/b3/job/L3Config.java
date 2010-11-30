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

import java.util.ArrayList;
import java.util.TreeSet;

/**
 * Creates the binning context from a job's configuration.
 *
 * @author Norman Fomferra
 */
public class L3Config {
    public static final String CONFNAME_L3_NUM_SCANS_PER_SLICE = "calvalus.l3.numScansPerSlice";
    public static final String CONFNAME_L3_GRID_NUM_ROWS = "calvalus.l3.grid.numRows";
    public static final String CONFNAME_L3_NUM_DAYS = "calvalus.l3.numDays";
    public static final String CONFNAME_L3_AGG_i_TYPE = "calvalus.l3.aggregators.%d.type";
    public static final String CONFNAME_L3_AGG_i_VAR_NAME = "calvalus.l3.aggregators.%d.varName";
    public static final String CONFNAME_L3_AGG_i_VAR_NAMES_j = "calvalus.l3.aggregators.%d.varNames.%d";
    public static final String CONFNAME_L3_AGG_i_WEIGHT_COEFF = "calvalus.l3.aggregators.%d.weightCoeff";
    public static final String CONFNAME_L3_MASK_EXPR = "calvalus.l3.maskExpr";
    public static final String CONFNAME_L3_VARIABLES_i_NAME = "calvalus.l3.variables.%d.name";
    public static final String CONFNAME_L3_VARIABLES_i_EXPR = "calvalus.l3.variables.%d.expr";
    public static final int DEFAULT_L3_NUM_SCANS_PER_SLICE = 64;
    public static final int DEFAULT_L3_NUM_NUM_DAYS = 16;

    public static BinningContext getBinningContext(Configuration conf) {
        VariableContext varCtx = getVariableContext(conf);
        return new BinningContextImpl(getBinningGrid(conf),
                                      varCtx,
                                      getBinManager(conf, varCtx));
    }

    public static BinningGrid getBinningGrid(Configuration conf) {
        int numRows = conf.getInt(CONFNAME_L3_GRID_NUM_ROWS, 2160);
        return new IsinBinningGrid(numRows);
    }

    public static BinManager getBinManager(Configuration conf) {
        return getBinManager(conf, getVariableContext(conf));
    }

    private static BinManager getBinManager(Configuration conf, VariableContext varCtx) {
        ArrayList<Aggregator> aggregators = new ArrayList<Aggregator>();
        for (int i = 0; ; i++) {
            String type = conf.get(String.format(CONFNAME_L3_AGG_i_TYPE, i));
            if (type == null) {
                break;
            }
            Aggregator aggregator;
            if (type.equals("AVG")) {
                aggregator = getAggregatorAverage(conf, varCtx, i);
            } else if (type.equals("AVG_ML")) {
                aggregator = getAggregatorAverageML(conf, varCtx, i);
            } else if (type.equals("MIN_MAX")) {
                aggregator = getAggregatorMinMax(conf, varCtx, i);
            } else if (type.equals("ON_MAX_SET")) {
                aggregator = getAggregatorOnMaxSet(conf, varCtx, i);
            } else {
                throw new IllegalArgumentException("Unknown aggregator type: " + type);
            }
            aggregators.add(aggregator);
        }
        return new BinManagerImpl(aggregators.toArray(new Aggregator[0]));
    }

    public static VariableContext getVariableContext(Configuration conf) {
        VariableContextImpl variableContext = new VariableContextImpl();

        variableContext.setMaskExpr(conf.get(CONFNAME_L3_MASK_EXPR));

        // define declared variables
        //
        for (int i = 0; ; i++) {
            String varName = conf.get(String.format(CONFNAME_L3_VARIABLES_i_NAME, i));
            String varExpr = conf.get(String.format(CONFNAME_L3_VARIABLES_i_EXPR, i));
            if (varName == null) {
                break;
            }
            variableContext.defineVariable(varName, varExpr);
        }

        // define variables of all aggregators
        //
        for (int i = 0; ; i++) {
            String aggType = conf.get(String.format(CONFNAME_L3_AGG_i_TYPE, i));
            if (aggType == null) {
                break;
            }
            String varName = conf.get(String.format(CONFNAME_L3_AGG_i_VAR_NAME, i));
            if (varName != null) {
                variableContext.defineVariable(varName);
            } else {
                for (int j = 0; ; j++) {
                    varName = conf.get(String.format(CONFNAME_L3_AGG_i_VAR_NAMES_j, i, j));
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

    private static Aggregator getAggregatorAverage(Configuration conf, VariableContext varCtx, int i) {
        String varName = conf.get(String.format(CONFNAME_L3_AGG_i_VAR_NAME, i));
        String weightCoeff = conf.get(String.format(CONFNAME_L3_AGG_i_WEIGHT_COEFF, i));
        if (weightCoeff == null) {
            return new AggregatorAverage(varCtx, varName);
        } else {
            return new AggregatorAverage(varCtx, varName, Double.parseDouble(weightCoeff));
        }
    }

    private static Aggregator getAggregatorAverageML(Configuration conf, VariableContext varCtx, int i) {
        String varName = conf.get(String.format(CONFNAME_L3_AGG_i_VAR_NAME, i));
        String weightCoeff = conf.get(String.format(CONFNAME_L3_AGG_i_WEIGHT_COEFF, i));
        if (weightCoeff == null) {
            return new AggregatorAverageML(varCtx, varName);
        } else {
            return new AggregatorAverageML(varCtx, varName, Double.parseDouble(weightCoeff));
        }
    }

    private static Aggregator getAggregatorMinMax(Configuration conf, VariableContext varCtx, int i) {
        String varName = conf.get(String.format(CONFNAME_L3_AGG_i_VAR_NAME, i));
        return new AggregatorMinMax(varCtx, varName);
    }

    private static Aggregator getAggregatorOnMaxSet(Configuration conf, VariableContext varCtx, int i) {
        ArrayList<String> varNames = new ArrayList<String>();
        for (int j = 0; ; j++) {
            String varName = conf.get(String.format(CONFNAME_L3_AGG_i_VAR_NAMES_j, i, j));
            if (varName == null) {
                break;
            }
            varNames.add(varName);
        }
        return new AggregatorOnMaxSet(varCtx, varNames.toArray(new String[0]));
    }
}
