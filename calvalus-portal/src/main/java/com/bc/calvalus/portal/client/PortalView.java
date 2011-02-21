package com.bc.calvalus.portal.client;

import com.google.gwt.user.client.ui.IsWidget;

/**
 * Base class for portal views.
 *
 * @author Norman
 */
public abstract class PortalView implements IsWidget {

    private final CalvalusPortal portal;

    public PortalView(CalvalusPortal portal) {
        this.portal = portal;
    }

    public CalvalusPortal getPortal() {
        return portal;
    }

    /**
     * Override to perform any operations that should be performed only if the portal is up and running.
     * The default implementation does nothing.
     */
    public void handlePortalStartedUp() {
    }

    public abstract int getViewId();

    public abstract String getTitle();

    public void show()  {
         getPortal().showView(getViewId());
    }
}