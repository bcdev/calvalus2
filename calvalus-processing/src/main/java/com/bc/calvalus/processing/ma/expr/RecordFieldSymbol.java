package com.bc.calvalus.processing.ma.expr;

import org.esa.snap.core.jexp.EvalEnv;

import java.lang.reflect.Field;

/**
 * A symbol that evaluates to fields of record values.
 *
 * @author Norman Fomferra
 */
public class RecordFieldSymbol extends RecordSymbol {
    private final String fieldName;
    private final String name;
    private Field field;

    public RecordFieldSymbol(String variableName, String fieldName) {
        super(variableName);
        this.fieldName = fieldName;
        this.name = variableName + "." + fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    protected Object getValue(EvalEnv env) {
        Object object = super.getValue(env);
        if (object != null) {
            return getFieldValue(object);
        }
        return object;
    }

    private Object getFieldValue(Object object) {
        if (field == null) {
            try {
                field = object.getClass().getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                throw new IllegalStateException(e);
            }
        }
        try {
            return field.get(object);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }


}