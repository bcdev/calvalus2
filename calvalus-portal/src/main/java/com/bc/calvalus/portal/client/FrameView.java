package com.bc.calvalus.portal.client;

import com.google.gwt.user.client.ui.Frame;
import com.google.gwt.user.client.ui.Widget;

/**
 * Simple IFRAME view.
 *
 * @author Norman
 */
public class FrameView extends PortalView {

    private int id;
    private String title;
    private String url;
    private Frame widget;

    public FrameView(CalvalusPortal portal, int id, String title, String url) {
        super(portal);
        this.id = id;
        this.title = title;
        this.url = url;

        widget = new Frame(url);
        widget.setHeight("800px");
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    @Override
    public int getViewId() {
        return id;
    }

    @Override
    public String getTitle() {
        return title;
    }
}