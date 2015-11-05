package com.bc.calvalus.processing.ma.expr;

import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.PixelExtractor;
import org.esa.snap.core.jexp.EvalEnv;
import org.esa.snap.core.jexp.EvalException;
import org.esa.snap.core.jexp.Function;
import org.esa.snap.core.jexp.Namespace;
import org.esa.snap.core.jexp.Symbol;
import org.esa.snap.core.jexp.Term;
import org.esa.snap.core.jexp.impl.AbstractFunction;
import org.esa.snap.core.jexp.impl.DefaultNamespace;

import java.util.Arrays;
import java.util.HashMap;

import static com.bc.calvalus.processing.ma.PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX;

/**
 * A namespace that is constructed from header names.
 *
 * @author Norman Fomferra
 */
public class HeaderNamespace implements Namespace {
    private final Header header;
    private final DefaultNamespace namespace;
    private final String[] attributeNames;
    private final HashMap<String, String> attributeTypes;

    private static final String ATT_TYPE_SCALAR = "scalar";
    private static final String ATT_TYPE_AGGREG = "aggreg";

    public HeaderNamespace(Header header) {
        this.header = header;
        this.namespace = new DefaultNamespace();
        String[] attributeNames = header.getAttributeNames();
        this.attributeNames = new String[attributeNames.length];
        this.attributeTypes = new HashMap<String, String>(attributeNames.length * 2);
        initAttributeNamesAndTypes(attributeNames);

        namespace.registerFunction(new AbstractFunction.D("median", -1) {
            @Override
            public double evalD(EvalEnv env, Term[] args) throws EvalException {
                final int n = args.length;
                if (n == 0) {
                    return 0.0;
                } else if (n == 1) {
                    return args[0].evalD(env);
                } else {
                    final double[] values = new double[n];
                    for (int i = 0; i < values.length; i++) {
                        values[i] = args[i].evalD(env);
                    }
                    Arrays.sort(values);
                    if (values.length % 2 == 1) {
                        return values[n / 2];
                    } else {
                        return 0.5 * (values[n / 2 - 1] + values[n / 2]);
                    }
                }
            }
        });
    }

    private void initAttributeNamesAndTypes(String[] attributeNames) {
        for (int i = 0; i < attributeNames.length; i++) {
            String attributeName = attributeNames[i];
            if (attributeName.startsWith(PixelExtractor.ATTRIB_NAME_AGGREG_PREFIX)) {
                String newName = attributeName.substring(ATTRIB_NAME_AGGREG_PREFIX.length());
                this.attributeNames[i] = newName;
                this.attributeTypes.put(newName, ATT_TYPE_AGGREG);
            } else {
                this.attributeNames[i] = attributeName;
                this.attributeTypes.put(attributeName, ATT_TYPE_SCALAR);
            }
        }
    }

    public String[] getAttributeNames() {
        return attributeNames;
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
        if (recordSymbol != null) {
            namespace.registerSymbol(recordSymbol);
        }
        return recordSymbol;
    }

    private RecordSymbol createSymbol(String name) {
        int pos = name.lastIndexOf('.');
        if (pos > 0) {
            String variableName = name.substring(0, pos);
            if (ATT_TYPE_AGGREG.equals(attributeTypes.get(variableName))) {
                String fieldName = name.substring(pos + 1);
                return new RecordFieldSymbol(variableName, fieldName);
            }
        }
        return attributeTypes.get(name) != null ? new RecordSymbol(name) : null;
    }

}
