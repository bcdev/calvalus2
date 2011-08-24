package com.bc.calvalus.processing.ma.expr;

import com.bc.jexp.EvalEnv;
import com.bc.jexp.EvalException;
import com.bc.jexp.Symbol;
import com.bc.jexp.Term;

/**
 * A symbol that evaluates to record values.
 *
 * @author Norman Fomferra
 */
public class RecordSymbol implements Symbol {
    private final String variableName;

    public RecordSymbol(String variableName) {
        this.variableName = variableName;
    }

    public String getVariableName() {
        return variableName;
    }

    @Override
    public String getName() {
        return getVariableName();
    }

    @Override
    public int getRetType() {
        return Term.TYPE_D;
    }

    @Override
    public boolean evalB(EvalEnv env) throws EvalException {
        return toBoolean(getValue(env));
    }

    @Override
    public int evalI(EvalEnv env) throws EvalException {
        return toInt(getValue(env));
    }

    @Override
    public double evalD(EvalEnv env) throws EvalException {
        return toDouble(getValue(env));
    }

    @Override
    public String evalS(EvalEnv env) throws EvalException {
        return toString(getValue(env));
    }

    public static boolean toBoolean(Object o) {
        if (o == null) {
            return false;
        } else if (o instanceof Boolean) {
            return (Boolean) o;
        } else if (o instanceof Number) {
            return ((Number) o).intValue() != 0;
        } else {
            return Boolean.parseBoolean(o.toString());
        }
    }

    public static double toDouble(Object o) {
        if (o == null) {
            return Double.NaN;
        } else if (o instanceof Number) {
            Number number = (Number) o;
            return number.doubleValue();
        } else if (o instanceof Boolean) {
            return ((Boolean) o) ? 1.0 : 0.0;
        } else {
            return Double.parseDouble(o.toString());
        }
    }

    public static int toInt(Object o) {
        if (o == null) {
            return 0;
        } else if (o instanceof Number) {
            return ((Number) o).intValue();
        } else if (o instanceof Boolean) {
            return ((Boolean) o) ? 1 : 0;
        } else {
            return Integer.parseInt(o.toString());
        }
    }

    public static String toString(Object o) {
        if (o == null) {
            return "";
        } else {
            return o.toString();
        }
    }

    protected Object getValue(EvalEnv env) {
        RecordEvalEnv recordEvalEnv = (RecordEvalEnv) env;
        return recordEvalEnv.getValue(variableName);
    }
}
