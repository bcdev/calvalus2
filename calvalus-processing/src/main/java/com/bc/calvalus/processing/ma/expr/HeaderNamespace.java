package com.bc.calvalus.processing.ma.expr;

import com.bc.calvalus.processing.ma.Header;
import com.bc.jexp.*;
import com.bc.jexp.impl.AbstractFunction;
import com.bc.jexp.impl.DefaultNamespace;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A namespace that is constructed from header names.
 *
 * @author Norman Fomferra
 */
public class HeaderNamespace implements Namespace {
    private final DefaultNamespace namespace;
    private final Header header;
    private Set<String> headerNames;

    public HeaderNamespace(Header header) {
        this.header = header;
        this.namespace = new DefaultNamespace();
        this.headerNames = new HashSet<String>(Arrays.asList(header.getAttributeNames()));

        namespace.registerFunction(new AbstractFunction.D("median", -1) {
            @Override
            public double evalD(EvalEnv env, Term[] args) throws EvalException {
                double[] values = new double[args.length];
                for (int i = 0; i < values.length; i++) {
                     values[i] = args[i].evalD(env);
                }
                return values[args.length / 2];
            }
        });
    }

    @Override
    public Function resolveFunction(String name, Term[] args) {
        return namespace.resolveFunction(name, args);
    }

    @Override
    public Symbol resolveSymbol(String name) {
        Symbol symbol = namespace.resolveSymbol(name);
        if (symbol != null) {
            return symbol;
        }
        RecordSymbol recordSymbol = createSymbol(name);
        if (!headerNames.contains(recordSymbol.getVariableName())) {
            return null;
        }
        namespace.registerSymbol(recordSymbol);
        return recordSymbol;
    }

    private RecordSymbol createSymbol(String name) {
        int pos = name.indexOf('.');
        if (pos > 0) {
            String variableName = name.substring(0, pos);
            String fieldName = name.substring(pos + 1);
            return new RecordFieldSymbol(variableName, fieldName);
        } else {
            return new RecordSymbol(name);
        }
    }
}
