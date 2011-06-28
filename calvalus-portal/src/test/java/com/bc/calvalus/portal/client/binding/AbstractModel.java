package com.bc.calvalus.portal.client.binding;

import com.google.gwt.event.shared.HandlerManager;
import com.google.gwt.event.shared.HandlerRegistration;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public abstract class AbstractModel implements Model {
    private HandlerManager handlerManager;

    protected AbstractModel() {
    }

    protected HandlerManager getHandlerManager() {
        if (handlerManager == null) {
            handlerManager = new HandlerManager(this);
        }
        return handlerManager;
    }

    protected void fireIfNotEqual(String name, Object oldValue, Object newValue) {
        if (handlerManager != null) {
            ModelChangeEvent.fireIfNotEqual(handlerManager, name, oldValue, newValue);
        }
    }

    @Override
    public void setValue(String name, Object value) {
        setValue(name, value, true);
    }

    @Override
    public HandlerRegistration addModelChangeHandler(ModelChangeHandler handler) {
        return getHandlerManager().addHandler(ModelChangeEvent.TYPE, handler);
    }
}
