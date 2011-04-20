package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProductSet;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.DecoratorPanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.IsWidget;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

import static com.bc.calvalus.portal.client.CalvalusPortal.*;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class InputOutputForm implements IsWidget {
    private ListBox inputProductSet;
    private ListBox outputFormat;
    private DecoratorPanel widget;
    private CheckBox autoStaging;

    public InputOutputForm(CalvalusPortal portal, String title, boolean hasOutput) {

        inputProductSet = new ListBox();
        inputProductSet.setName("inputProductSet");
        for (GsProductSet productSet : portal.getProductSets()) {
            inputProductSet.addItem(productSet.getName(), productSet.getId());
        }
        inputProductSet.setWidth("20em");
        inputProductSet.setVisibleItemCount(6);

        outputFormat = new ListBox();
        outputFormat.setName("outputFormat");
        outputFormat.addItem("BEAM-DIMAP");
        outputFormat.addItem("NetCDF");
        outputFormat.addItem("GeoTIFF");
        outputFormat.setWidth("20em");
        outputFormat.setVisibleItemCount(1);
        outputFormat.setSelectedIndex(0);

        autoStaging = new CheckBox("Perform staging step after successful production");
        autoStaging.setValue(true);

        HorizontalPanel outputControlPanel = new HorizontalPanel();
        outputControlPanel.setSpacing(2);
        outputControlPanel.add(createLabeledWidgetV("Output format:", outputFormat));
        outputControlPanel.add(autoStaging);

        FlexTable layout = new FlexTable();
        layout.setWidth("100%");
        layout.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        layout.getFlexCellFormatter().setHorizontalAlignment(2, 0, HasHorizontalAlignment.ALIGN_CENTER);
        layout.setCellSpacing(4);
        layout.setWidget(0, 0, new HTML("<b>Input</b>"));
        layout.setWidget(1, 0, createLabeledWidgetV("Input product file set:", inputProductSet));
        if (hasOutput) {
            layout.setWidget(2, 0, new HTML("<b>Output</b>"));
            layout.setWidget(3, 0, createLabeledWidgetV("Output product file format:", outputFormat));
            layout.setWidget(4, 0, autoStaging);
        }

        // Wrap the contents in a DecoratorPanel
        widget = new DecoratorPanel();
        widget.setTitle(title); //todo - check why title doesn't show
        widget.setWidget(layout);
    }

    public String getInputProductSetId() {
        return inputProductSet.getValue(inputProductSet.getSelectedIndex());
    }

    public String getOutputFormat() {
        int index = outputFormat.getSelectedIndex();
        return outputFormat.getValue(index);
    }

    public boolean isAutoStaging() {
        return autoStaging.getValue();
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    public void validateForm() {
    }
}