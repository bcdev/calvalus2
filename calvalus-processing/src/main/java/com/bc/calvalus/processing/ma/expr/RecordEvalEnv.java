package com.bc.calvalus.processing.ma.expr;

import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.Record;
import com.bc.jexp.EvalEnv;

import java.util.HashMap;

/**
* Environment for evaluating expressions that use a record's attribute values.
*
* @author Norman Fomferra
*/
public class RecordEvalEnv implements EvalEnv {

    private final Header header;
    private final HashMap<String, Object> env;

    public RecordEvalEnv(Header header) {
        this.header = header;
        this.env = new HashMap<String, Object>(31);
    }

    public void setValues(Record record) {
        String[] names = header.getAttributeNames();
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
