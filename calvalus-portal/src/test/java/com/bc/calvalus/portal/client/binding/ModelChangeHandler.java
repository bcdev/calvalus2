package com.bc.calvalus.portal.client.binding;

import com.google.gwt.event.shared.EventHandler;

/**
* A listener that is called when a model has changed.
*
* @author Norman Fomferra
*/
public interface ModelChangeHandler extends EventHandler {
    /**
     * Called when a {@link ModelChangeEvent} is fired.
     *
     * @param event the {@link ModelChangeEvent} that was fired
     */
    void onModelChange(ModelChangeEvent event);
}
