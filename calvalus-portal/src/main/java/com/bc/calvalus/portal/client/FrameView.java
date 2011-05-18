package com.bc.calvalus.portal.client;

import com.google.gwt.user.client.ui.Frame;
import com.google.gwt.user.client.ui.Widget;

/**
 * Simple IFRAME view.
 *
 * @author Norman
 */
public class FrameView extends PortalView {

    private String id;
    private String title;
    private Frame widget;

    public FrameView(PortalContext portalContext, String id, String title, String url) {
        super(portalContext);
        this.id = id;
        this.title = title;

        widget = new Frame(url);
        widget.setWidth("100%");
        widget.setHeight("800px");
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    @Override
    public String getViewId() {
        return id;
    }

    @Override
    public String getTitle() {
        return title;
    }
}