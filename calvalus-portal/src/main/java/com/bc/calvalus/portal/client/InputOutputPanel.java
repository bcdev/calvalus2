package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProductSet;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.DecoratorPanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.IsWidget;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import static com.bc.calvalus.portal.client.CalvalusPortal.*;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class InputOutputPanel implements IsWidget {
    private ListBox inputProductSet;
    private ListBox outputFormat;
    private DecoratorPanel widget;
    private TextBox outputFileName;
    private CheckBox outputStaging;

    public InputOutputPanel(CalvalusPortal portal, String title) {

        inputProductSet = new ListBox();
        inputProductSet.setName("inputProductSet");
        for (PortalProductSet productSet : portal.getProductSets()) {
            inputProductSet.addItem(productSet.getName(), productSet.getId());
        }
        inputProductSet.setWidth("20em");
        inputProductSet.setVisibleItemCount(6);

        outputFileName = new TextBox();
        outputFileName.setName("outputFileName");
        outputFileName.setText("output-${user}-${num}");
        outputFileName.setWidth("20em");

        outputFormat = new ListBox();
        outputFormat.setName("outputFormat");
        outputFormat.addItem("BEAM-DIMAP");
        outputFormat.addItem("NetCDF");
        outputFormat.addItem("GeoTIFF");
        outputFormat.setWidth("20em");
        outputFormat.setVisibleItemCount(1);
        outputFormat.setSelectedIndex(0);

        outputStaging = new CheckBox("Perform staging step after successful production");
        outputStaging.setValue(true);

        HorizontalPanel outputControlPanel = new HorizontalPanel();
        outputControlPanel.setSpacing(2);
        outputControlPanel.add(createLabeledWidgetV("Output format:", outputFormat));
        outputControlPanel.add(outputStaging);

        FlexTable layout = new FlexTable();
        layout.setWidth("100%");
        layout.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        layout.getFlexCellFormatter().setHorizontalAlignment(2, 0, HasHorizontalAlignment.ALIGN_CENTER);
        layout.setCellSpacing(4);
        layout.setWidget(0, 0, new HTML("<b>Input</b>"));
        layout.setWidget(1, 0, createLabeledWidgetV("Input product set:", inputProductSet));
        layout.setWidget(2, 0, new HTML("<b>Output</b>"));
        layout.setWidget(3, 0, createLabeledWidgetV("Output file name:", outputFileName));
        layout.setWidget(4, 0, createLabeledWidgetV("Output format:", outputFormat));
        layout.setWidget(5, 0, outputStaging);

        // Wrap the contents in a DecoratorPanel
        widget = new DecoratorPanel();
        widget.setTitle(title); //todo - check why title doesn't show
        widget.setWidget(layout);
    }

    public String getInputProductSetId() {
        return inputProductSet.getValue(inputProductSet.getSelectedIndex());
    }

    public String getOutputFileName() {
        return outputFileName.getText();
    }

    public String getOutputFormat() {
        int index = outputFormat.getSelectedIndex();
        return outputFormat.getValue(index);
    }

    public boolean isOutputStaging() {
        return outputStaging.getValue();
    }

    @Override
    public Widget asWidget() {
        return widget;
    }
}