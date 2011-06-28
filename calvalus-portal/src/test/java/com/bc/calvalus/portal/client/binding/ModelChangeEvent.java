package com.bc.calvalus.portal.client.binding;

import com.google.gwt.event.shared.GwtEvent;
import com.google.gwt.event.shared.HasHandlers;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public class ModelChangeEvent extends GwtEvent<ModelChangeHandler> {
    /**
     * Handler type.
     */
    public final static Type<ModelChangeHandler> TYPE = new Type<ModelChangeHandler>();

    private final String name;
    private final Object value;

    public static void fireIfNotEqual(HasHandlers source, String name, Object oldValue, Object newValue) {
        if (oldValue != newValue && (oldValue == null || !oldValue.equals(newValue))) {
            source.fireEvent(new ModelChangeEvent(name, newValue));
        }
    }

    private ModelChangeEvent(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    @Override
    protected void dispatch(ModelChangeHandler handler) {
        handler.onModelChange(this);
    }

    @Override
    public Type<ModelChangeHandler> getAssociatedType() {
        return TYPE;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }
}
