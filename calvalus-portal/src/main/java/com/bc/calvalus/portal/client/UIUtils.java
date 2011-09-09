package com.bc.calvalus.portal.client;

import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

/**
 * GUI utilities.
 *
 * @author Norman
 */
public class UIUtils {

    public static VerticalPanel createVerticalPanel(Widget... widgets) {
        return createVerticalPanel(0, widgets);
    }

    public static VerticalPanel createVerticalPanel(int spacing, Widget... widgets) {
        VerticalPanel panel = new VerticalPanel();
        panel.setSpacing(spacing);
        for (Widget widget : widgets) {
            panel.add(widget);
        }
        return panel;
    }

    public static HorizontalPanel createHorizontalPanel(Widget... widgets) {
        return createHorizontalPanel(0, widgets);
    }

    public static HorizontalPanel createHorizontalPanel(int spacing, Widget... widgets) {
        HorizontalPanel panel = new HorizontalPanel();
        panel.setSpacing(spacing);
        for (Widget widget : widgets) {
            panel.add(widget);
        }
        return panel;
    }
}
