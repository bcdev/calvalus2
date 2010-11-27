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

    public static BinningContext getBinningContext(Configuration conf) {
        VariableContext varCtx = getVariableContext(conf);
        return new BinningContextImpl(getBinningGrid(conf),
                                      varCtx,
                                      getBinManager(conf, varCtx));
    }

    public static BinningGrid getBinningGrid(Configuration conf) {
        int numRows = conf.getInt("calvalus.l3.grid.numRows", 2160);
        return new IsinBinningGrid(numRows);
    }

    public static BinManager getBinManager(Configuration conf) {
        return getBinManager(conf, getVariableContext(conf));
    }

    private static BinManager getBinManager(Configuration conf, VariableContext varCtx) {
        ArrayList<Aggregator> aggregators = new ArrayList<Aggregator>();
        for (int i = 0; ; i++) {
            String aggPrefix = "calvalus.l3.aggregators." + i;
            String type = conf.get(aggPrefix + ".type");
            if (type == null) {
                break;
            }
            Aggregator aggregator = null;
            if (type.equals("AVG")) {
                aggregator = getAggregatorAverage(conf, varCtx, aggPrefix);
            } else if (type.equals("AVG_ML")) {
                aggregator = getAggregatorAverageML(conf, varCtx, aggPrefix);
            } else if (type.equals("MIN_MAX")) {
                aggregator = getAggregatorMinMax(conf, varCtx, aggPrefix);
            } else if (type.equals("ON_MAX_SET")) {
                aggregator = getAggregatorOnMaxSet(conf, varCtx, aggPrefix);
            } else {
                throw new IllegalArgumentException("Unknown aggregator type: " + type);
            }
            aggregators.add(aggregator);
        }
        return new BinManagerImpl(aggregators.toArray(new Aggregator[0]));
    }

    public static VariableContext getVariableContext(Configuration conf) {
        TreeSet<String> varNames = new TreeSet<String>();
        for (int i = 0; ; i++) {
            String virtualBandName = conf.get("calvalus.l3.virtualBand." + i + ".name");
            if (virtualBandName == null) {
                break;
            }
            varNames.add(virtualBandName);
        }
        for (int i = 0; ; i++) {
            String aggPrefix = "calvalus.l3.aggregators." + i;
            String aggType = conf.get(aggPrefix + ".type");
            if (aggType == null) {
                break;
            }
            String varName = conf.get(aggPrefix + ".varName");
            if (varName != null) {
                varNames.add(varName);
            } else {
                for (int j = 0; ; j++) {
                    varName = conf.get(aggPrefix + ".varNames." + j);
                    if (varName != null) {
                        varNames.add(varName);
                    } else {
                        break;
                    }
                }
            }
        }
        return new VariableContextImpl(varNames.toArray(new String[0]));
    }

    private static Aggregator getAggregatorAverage(Configuration conf, VariableContext varCtx, String aggPrefix) {
        String varName = conf.get(aggPrefix + ".varName");
        String weightCoeff = conf.get(aggPrefix + ".weightCoeff");
        if (weightCoeff == null) {
            return new AggregatorAverage(varCtx, varName);
        } else {
            return new AggregatorAverage(varCtx, varName, Double.parseDouble(weightCoeff));
        }
    }

    private static Aggregator getAggregatorAverageML(Configuration conf, VariableContext varCtx, String aggPrefix) {
        String varName = conf.get(aggPrefix + ".varName");
        String weightCoeff = conf.get(aggPrefix + ".weightCoeff");
        if (weightCoeff == null) {
            return new AggregatorAverageML(varCtx, varName);
        } else {
            return new AggregatorAverageML(varCtx, varName, Double.parseDouble(weightCoeff));
        }
    }

    private static Aggregator getAggregatorMinMax(Configuration conf, VariableContext varCtx, String aggPrefix) {
        String varName = conf.get(aggPrefix + ".varName");
        return new AggregatorMinMax(varCtx, varName);
    }

    private static Aggregator getAggregatorOnMaxSet(Configuration conf, VariableContext varCtx, String aggPrefix) {
        ArrayList<String> varNames = new ArrayList<String>();
        for (int i = 0; ; i++) {
            String varName = conf.get(aggPrefix + ".varNames." + i);
            if (varName == null) {
                break;
            }
            varNames.add(varName);
        }
        return new AggregatorOnMaxSet(varCtx, varNames.toArray(new String[0]));
    }
}
