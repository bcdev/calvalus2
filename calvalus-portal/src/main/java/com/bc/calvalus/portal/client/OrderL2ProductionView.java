package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProcessorDescriptor;
import com.bc.calvalus.portal.shared.GsProductionRequest;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class OrderL2ProductionView extends PortalView {
    public static final int ID = 1;
    private InputOutputPanel inputOutputPanel;
    private GeneralProcessorPanel processingPanel;
    private L2ProductFilterPanel productFilterPanel;
    private FlexTable widget;

    public OrderL2ProductionView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        inputOutputPanel = new InputOutputPanel(calvalusPortal, "L2 Input/Output");
        processingPanel = new GeneralProcessorPanel(getPortal(), "L2 Processor");
        productFilterPanel = new L2ProductFilterPanel();

        widget = new FlexTable();
        widget.getFlexCellFormatter().setVerticalAlignment(0, 0, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setVerticalAlignment(0, 1, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setVerticalAlignment(1, 0, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setHorizontalAlignment(2, 0, HasHorizontalAlignment.ALIGN_RIGHT);
        widget.getFlexCellFormatter().setColSpan(2, 0, 2);
        widget.getFlexCellFormatter().setRowSpan(0, 1, 0);
        widget.ensureDebugId("cwFlexTable");
        widget.addStyleName("cw-FlexTable");
        widget.setCellSpacing(2);
        widget.setCellPadding(2);
        widget.setWidget(0, 0, inputOutputPanel.asWidget());
        widget.setWidget(0, 1, processingPanel.asWidget());
        widget.setWidget(1, 0, productFilterPanel.asWidget());
        widget.setWidget(2, 0, new Button("Order Production", new OrderProductionHandler()));
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    @Override
    public int getViewId() {
        return ID;
    }

    @Override
    public String getTitle() {
        return "Order L2 Production";
    }

    // todo - Provide JUnit test for this method
    public GsProductionRequest getProductionRequest() {
        return new GsProductionRequest("calvalus-level2", "ewa", getValueMap());
    }

    // todo - Provide JUnit test for this method
    public HashMap<String, String> getValueMap() {
        GsProcessorDescriptor selectedProcessor = processingPanel.getSelectedProcessor();
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputProductSetId", inputOutputPanel.getInputProductSetId());
        parameters.put("outputFormat", inputOutputPanel.getOutputFormat());
        parameters.put("autoStaging", inputOutputPanel.isAutoStaging() + "");
        parameters.put("processorBundleName", selectedProcessor.getBundleName());
        parameters.put("processorBundleVersion", processingPanel.getBundleVersion());
        parameters.put("processorName", selectedProcessor.getExecutableName());
        parameters.put("processorParameters", processingPanel.getProcessorParameters());
        parameters.putAll(productFilterPanel.getValueMap());
        return parameters;
    }

    private class OrderProductionHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            GsProductionRequest request = getProductionRequest();
            getPortal().orderProduction(request);
        }
    }

}