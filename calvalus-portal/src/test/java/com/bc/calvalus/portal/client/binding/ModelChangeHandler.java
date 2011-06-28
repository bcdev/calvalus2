package com.bc.calvalus.portal.client.binding;

import com.google.gwt.event.shared.EventHandler;

/**
* todo - add api doc
*
* @author Norman Fomferra
*/
public interface ModelChangeHandler extends EventHandler {
    /**
     * Called when {@link com.google.gwt.event.logical.shared.ValueChangeEvent} is fired.
     *
     * @param event the {@link com.google.gwt.event.logical.shared.ValueChangeEvent} that was fired
     */
    void onModelChange(ModelChangeEvent event);
}
