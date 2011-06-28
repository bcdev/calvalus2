package com.bc.calvalus.portal.client.binding;

import com.google.gwt.event.shared.HandlerRegistration;

/**
* todo - add api doc
*
* @author Norman Fomferra
*/
public interface Model {

    Object getValue(String name);

    void setValue(String name, Object value);

    void setValue(String name, Object value, boolean fireEvents);

    HandlerRegistration addModelChangeHandler(ModelChangeHandler handler);
}
