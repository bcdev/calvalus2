package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GUI utilities.
 *
 * @author Norman
 * @author Hans
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
            List<String> productIdentifiersFromCatalogue = inputSelection.getProductIdentifiers();
            List<String> productIdentifiers = new ArrayList<>();
            if (productIdentifiersFromCatalogue != null) {
                // this is to anticipate the product identifiers coming from catalogue, which
                // looks like EOP:CODE-DE:S2_MSI_L1C:/S2A_MSIL1C_20171118T111341_N0206_R137_T30UYC_20171118T113620
                for (String s : productIdentifiersFromCatalogue) {
                    if (s.contains("/")) {
                        productIdentifiers.add(s.substring(s.lastIndexOf("/") + 1));
                    } else {
                        productIdentifiers.add(s);
                    }
                }
                parameters.put("productIdentifiers", String.join(",", productIdentifiers));
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
            String regionGeometry = inputSelection.getRegionGeometry();
            if (regionGeometry != null && regionGeometry.length() > 0) {
                parameters.put("regionWKT", regionGeometry);
            }
            parameters.put("geoInventory", inputSelection.getCollectionName());
            parameters.put("collectionName", inputSelection.getCollectionName());
            parameters.put("warningMessage", inputSelection.getWarningMessage());
        }
        return parameters;
    }
}
