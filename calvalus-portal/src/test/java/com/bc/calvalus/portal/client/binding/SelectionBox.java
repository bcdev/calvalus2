package com.bc.calvalus.portal.client.binding;

import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.event.shared.GwtEvent;
import com.google.gwt.event.shared.HandlerManager;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.i18n.client.HasDirection;
import com.google.gwt.user.client.ui.HasValue;
import com.google.gwt.user.client.ui.ListBox;

import java.util.ArrayList;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public class SelectionBox<T> extends ListBox implements HasValue<T> {

    private HandlerManager handlerManager;
    private ArrayList<T> items;
    private T value;

    public SelectionBox() {
        super(false);
        items = new ArrayList<T>();
        addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                int index = getSelectedIndex();
                if (index != -1) {
                    T newValue = items.get(index);
                    if (value != newValue) {
                        setValue(newValue, true);
                    }
                } else {
                    setValue(null, true);
                }
            }
        });
    }

    public void addItem(String itemText, T itemValue) {
        items.add(itemValue);
        super.addItem(itemText);
    }

    @Override
    public void removeItem(int index) {
        items.remove(index);
        super.removeItem(index);
    }

    @Override
    public void clear() {
        items.clear();
        super.clear();
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public void setValue(T newValue) {
        setValue(newValue, false);
    }

    @Override
    public void setValue(T newValue, boolean fireEvents) {
        T oldValue = getValue();

        if (newValue != null) {
            int index = items.indexOf(newValue);
            if (index == -1) {
                throw new IllegalArgumentException("Value is not a valid item: " + newValue);
            }
            this.value = newValue;
            if (index != getSelectedIndex()) {
                setSelectedIndex(index);
            }
        } else {
            this.value = null;
        }

        if (fireEvents && handlerManager != null) {
            ValueChangeEvent.fireIfNotEqual(this, oldValue, value);
        }
    }

    @Override
    public void fireEvent(GwtEvent<?> event) {
        if (event.getAssociatedType() == ValueChangeEvent.getType()) {
            if (handlerManager != null) {
                handlerManager.fireEvent(event);
            }
        } else {
            super.fireEvent(event);
        }
    }

    @Override
    public HandlerRegistration addValueChangeHandler(ValueChangeHandler<T> valueChangeHandler) {
        return ensureHandlers().addHandler(ValueChangeEvent.getType(), valueChangeHandler);
    }

    /**
     * Ensures the existence of the handler manager.
     *
     * @return the handler manager
     */
    HandlerManager ensureHandlers() {
        return handlerManager == null ? handlerManager = createHandlerManager()
                : handlerManager;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Unsupported operations

    @Override
    public void addItem(String item) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addItem(String item, HasDirection.Direction dir) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addItem(String item, HasDirection.Direction dir, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addItem(String item, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertItem(String item, HasDirection.Direction dir, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertItem(String item, HasDirection.Direction dir, String value, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertItem(String item, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertItem(String item, String value, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setValue(int index, String value) {
        throw new UnsupportedOperationException();
    }
}
