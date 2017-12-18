package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * GUI utilities.
 *
 * @author Norman
 */
class UIUtils {

    static VerticalPanel createVerticalPanel(Widget... widgets) {
        return createVerticalPanel(0, widgets);
    }

    static VerticalPanel createVerticalPanel(int spacing, Widget... widgets) {
        VerticalPanel panel = new VerticalPanel();
        panel.setSpacing(spacing);
        for (Widget widget : widgets) {
            panel.add(widget);
        }
        return panel;
    }

    static Map<String, String> parseParametersFromContext(DtoInputSelection inputSelection) {
        Map<String, String> parameters = new HashMap<>();
        if (inputSelection != null) {
            if (inputSelection.getProductIdentifiers() != null) {
                parameters.put("productIdentifiers", String.join(",", inputSelection.getProductIdentifiers()));
            } else {
                parameters.put("productIdentifiers", "");
            }

            String startTime = null;
            String endTime = null;
            if (inputSelection.getDateRange() != null) {
                startTime = inputSelection.getDateRange().getStartTime();
                startTime = startTime.split("T")[0];
                endTime = inputSelection.getDateRange().getEndTime();
                endTime = endTime.split("T")[0];
            }
            parameters.put("minDate", startTime);
            parameters.put("maxDate", endTime);
            parameters.put("regionWKT", inputSelection.getRegionGeometry());

            parameters.put("geoInventory", inputSelection.getCollectionName());
            parameters.put("collectionName", inputSelection.getCollectionName());
        }
        return parameters;
    }
}
