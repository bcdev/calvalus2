package com.bc.calvalus.production;

import com.bc.ceres.binding.ConversionException;
import com.bc.ceres.binding.ValidationException;
import com.bc.ceres.binding.dom.DomConverter;
import com.bc.ceres.binding.dom.DomElement;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marco Peters
 */
public class HashMapDomConverter implements DomConverter {

    @Override
    public Class<?> getValueType() {
        return HashMap.class;
    }

    @Override
    public Object convertDomToValue(DomElement parentElement, Object value) throws ConversionException,
                                                                                   ValidationException {
        HashMap map;
        if (value instanceof HashMap) {
            map = (HashMap) value;
        } else {
            map = new HashMap();
        }
        DomElement[] children = parentElement.getChildren();
        for (DomElement child : children) {
            String entryKey = child.getName();
            String entryValue = child.getValue();
            map.put(entryKey, entryValue);
        }
        return map;
    }

    @Override
    public void convertValueToDom(Object value, DomElement parentElement) throws ConversionException {
        HashMap<String, String> map = (HashMap<String, String>) value;
        Set<Map.Entry<String, String>> entrySet = map.entrySet();
        for (Map.Entry<String, String> entry : entrySet) {
            DomElement entryElement = parentElement.createChild(entry.getKey());
            entryElement.setValue(entry.getValue());
        }
    }
}
