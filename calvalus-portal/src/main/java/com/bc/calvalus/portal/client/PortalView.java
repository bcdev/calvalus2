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

    /**
     * Override to perform any operations that should be performed only if the portal is up and running.
     * The default implementation does nothing.
     */
    public void handlePortalStartedUp() {
    }

    public String getViewId() {
        return getClass().getName();
    }

    public abstract String getTitle();
}