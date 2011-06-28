package com.bc.calvalus.portal.client.binding;

import com.google.gwt.event.shared.HandlerManager;
import com.google.gwt.event.shared.HandlerRegistration;

import java.util.HashMap;
import java.util.Map;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public class DefaultModel extends AbstractModel {

    final private Map<String, Object> values;

    public DefaultModel() {
        this.values = new HashMap<String, Object>();
    }

    public DefaultModel(Map<String, Object> values) {
        this.values = new HashMap<String, Object>(values);
    }

    @Override
    public Object getValue(String name) {
        return values.get(name);
    }

    @Override
    public void setValue(String name, Object newValue, boolean fireEvents) {
        Object oldValue = values.get(name);
        if (newValue != null) {
            values.put(name, newValue);
        } else {
            values.remove(name);
        }
        if (fireEvents) {
            fireIfNotEqual(name, oldValue, newValue);
        }
    }


}
