package com.bc.calvalus.portal.client.binding;

import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.user.client.ui.HasValue;

import java.util.HashMap;
import java.util.Map;

/**
* todo - add api doc
*
* @author Norman Fomferra
*/
public class BindingContext {
    Model model;
    Map<String, BindingImpl> bindings;

    public BindingContext(Model model) {
        this.model = model;
        bindings = new HashMap<String, BindingImpl>();
    }

    public <T> Binding<T> bind(final String name, final HasValue<T> widget) {
        return bind(name, widget, true);
    }

    public <T> Binding<T> bind(final String name, final HasValue<T> widget, final boolean fireEvents) {
        HandlerRegistration handlerRegistration1 = widget.addValueChangeHandler(new ValueChangeHandler<T>() {
            @Override
            public void onValueChange(ValueChangeEvent<T> valueChangeEvent) {
                model.setValue(name, valueChangeEvent.getValue(), fireEvents);
            }
        });
        HandlerRegistration handlerRegistration2 = model.addModelChangeHandler(new ModelChangeHandler() {
            @Override
            public void onModelChange(ModelChangeEvent event) {
                widget.setValue((T) event.getValue(), fireEvents);
            }
        });
        BindingImpl<T> binding = new BindingImpl<T>(name, widget, handlerRegistration1, handlerRegistration2);
        bindings.put(name, binding);
        return binding;
    }

    public void unbind(String name) {
        BindingImpl binding = bindings.get(name);
        if (binding != null) {
            binding.unbind();
            bindings.remove(name);
        }
    }

    private static class BindingImpl<T> implements Binding<T> {
        private String name;
        private HasValue<T> widget;
        private final HandlerRegistration[] handlerRegistrations;

        private BindingImpl(String name, HasValue<T> widget, HandlerRegistration ... handlerRegistrations) {
            this.name = name;
            this.widget = widget;
            this.handlerRegistrations = handlerRegistrations;
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            unbind();
        }

        @Override
        public String getName() {
            return name;
        }
        @Override
        public HasValue<T> getWidget() {
            return widget;
        }

        @Override
        public Binding<T> match(String pattern) {
            // todo
            return this;
        }

        @Override
        public Binding<T> inRange(T v1, T v2) {
            // todo
            return this;
        }

        @Override
        public Binding<T> notEmpty() {
            // todo
            return this;
        }

        public void unbind() {
            for (HandlerRegistration handlerRegistration: handlerRegistrations) {
                handlerRegistration.removeHandler();
            }
        }
    }
}
