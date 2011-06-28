package com.bc.calvalus.portal.client;

import com.google.gwt.user.client.ui.IsWidget;

// todo - lets this class be a 'LazyPanel'

/**
 * Base class for portal views.
 *
 * @author Norman
 */
public abstract class PortalView implements IsWidget {

    private final PortalContext portalContext;

    public PortalView(PortalContext portalContext) {
        this.portalContext = portalContext;
    }

    public PortalContext getPortal() {
        return portalContext;
    }

    public String getViewId() {
        return getClass().getName();
    }

    public abstract String getTitle();

    /**
     * Informs the view that it is now shown.
     * The default implementation does nothing.
     */
    public void onShowing() {
    }

    /**
     * Informs the view that it is now hidden.
     * The default implementation does nothing.
     */
    public void onHidden() {
    }
}