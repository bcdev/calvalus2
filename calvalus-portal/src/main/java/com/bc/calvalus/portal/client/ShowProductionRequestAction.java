package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.ScrollPanel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static org.geotools.gml3.GML.result;

/**
 * @author Norman
 * @since 0.3
 */
public class ShowProductionRequestAction {

    public static void run(String title, DtoProductionRequest result) {
        FlexTable flexTable = new FlexTable();
        fillTable(flexTable, result);
        ScrollPanel scrollPanel = new ScrollPanel(flexTable);
        scrollPanel.setWidth("640px");
        scrollPanel.setHeight("480px");
        Dialog.info(title, scrollPanel);
    }

    public static void fillTable(FlexTable flexTable, DtoProductionRequest result) {
        FlexTable.FlexCellFormatter flexCellFormatter = flexTable.getFlexCellFormatter();
        flexCellFormatter.setColSpan(0, 0, 2);
        flexTable.setCellSpacing(5);
        flexTable.setCellPadding(3);
        flexTable.setHTML(0, 0, "<i>Production type: " + result.getProductionType() + "</i>");
        flexTable.setHTML(1, 0, "<b>Parameter Name</b>");
        flexTable.setHTML(1, 1, "<b>Parameter Value</b>");
        Map<String, String> productionParameters = result.getProductionParameters();
        ArrayList<String> names = new ArrayList<String>(productionParameters.keySet());
        Collections.sort(names);
        int i = 2;
        for (String name : names) {
            flexTable.setHTML(i, 0, name + ":");
            String value = productionParameters.get(name).replaceAll("<", "&lt;").replaceAll(">", "&gt;");
            flexTable.setHTML(i, 1, "<pre>" + value + "</pre>");
            i++;
        }
    }
}
