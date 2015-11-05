package com.bc.calvalus.processing.ma.expr;

import com.bc.calvalus.processing.ma.Record;
import org.esa.snap.core.jexp.EvalEnv;

import java.util.HashMap;

/**
* Environment for evaluating expressions that use a record's attribute values.
*
* @author Norman Fomferra
*/
public class RecordEvalEnv implements EvalEnv {

    private final HeaderNamespace namespace;
    private final HashMap<String, Object> env;

    public RecordEvalEnv(HeaderNamespace namespace) {
        this.namespace = namespace;
        this.env = new HashMap<String, Object>(31);
    }

    public void setContext(Record record) {
        String[] names = namespace.getAttributeNames();
        Object[] values = record.getAttributeValues();
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            Object value = values[i];
            env.put(name, value);
        }
    }

    public Object getValue(String name) {
        return  env.get(name);
    }
}
