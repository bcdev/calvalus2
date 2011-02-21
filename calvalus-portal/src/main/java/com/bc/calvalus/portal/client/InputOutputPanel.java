package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProductSet;
import com.google.gwt.user.client.ui.DecoratorPanel;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.IsWidget;
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
    private TextBox outputFileName;
    private DecoratorPanel widget;

    public InputOutputPanel(CalvalusPortal portal, String title) {

        inputProductSet = new ListBox();
        inputProductSet.setName("inputProductSet");
        for (PortalProductSet productSet : portal.getProductSets()) {
            inputProductSet.addItem(productSet.getName(), productSet.getId());
        }
        inputProductSet.setVisibleItemCount(6);

        outputFileName = new TextBox();
        outputFileName.setName("outputFileName");
        outputFileName.setText("output-${user}-${num}");

        FlexTable layout = new FlexTable();
        layout.setWidth("100%");
        layout.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        layout.setCellSpacing(4);
        layout.setWidget(0, 0, new HTML("<b>Input</b>"));
        layout.setWidget(1, 0, createLabeledWidgetV("Input product set:", inputProductSet));
        layout.setWidget(2, 0, new HTML("<b>Output</b>"));
        layout.setWidget(3, 0, createLabeledWidgetV("Output file name:", outputFileName));

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

    @Override
    public Widget asWidget() {
        return widget;
    }
}