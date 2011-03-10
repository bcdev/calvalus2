package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProcessorDescriptor;
import com.bc.calvalus.portal.shared.GsProductionRequest;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Window;
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
public class OrderL3ProductionView extends PortalView {
    public static final int ID = 2;
    private FlexTable widget;
    private InputOutputPanel inputOutputPanel;
    private GeneralProcessorPanel l2ProcessorPanel;
    private L3ParametersPanel l3ParametersPanel;

    public OrderL3ProductionView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        inputOutputPanel = new InputOutputPanel(getPortal(), "L3 Input/Output");
        l2ProcessorPanel = new GeneralProcessorPanel(getPortal(), "L2 Processor");
        l3ParametersPanel = new L3ParametersPanel();

        widget = new FlexTable();
        widget.getFlexCellFormatter().setVerticalAlignment(0, 0, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setVerticalAlignment(0, 1, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setVerticalAlignment(1, 0, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setVerticalAlignment(1, 1, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setHorizontalAlignment(2, 0, HasHorizontalAlignment.ALIGN_RIGHT);
        widget.getFlexCellFormatter().setColSpan(2, 0, 2);
        widget.getFlexCellFormatter().setRowSpan(0, 1, 2);
        widget.ensureDebugId("cwFlexTable");
        widget.addStyleName("cw-FlexTable");
        widget.setWidth("32em");
        widget.setCellSpacing(2);
        widget.setCellPadding(2);
        widget.setWidget(0, 0, inputOutputPanel.asWidget());
        widget.setWidget(1, 0, l2ProcessorPanel.asWidget());
        widget.setWidget(0, 1, l3ParametersPanel.asWidget());
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
        return "Order L3 Production";
    }

    // todo - Provide JUnit test for this method
    public GsProductionRequest getProductionRequest() {
        HashMap<String, String> parameters = getValueMap();
        return new GsProductionRequest("calvalus-level3", parameters);
    }

    // todo - Provide JUnit test for this method
    public HashMap<String, String> getValueMap() {
        GsProcessorDescriptor selectedProcessor = l2ProcessorPanel.getSelectedProcessor();
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputProductSetId", inputOutputPanel.getInputProductSetId());
        parameters.put("outputFormat", inputOutputPanel.getOutputFormat());
        parameters.put("autoStaging", inputOutputPanel.isAutoStaging() + "");
        parameters.put("l2ProcessorBundleName", selectedProcessor.getBundleName());
        parameters.put("l2ProcessorBundleVersion", l2ProcessorPanel.getBundleVersion());
        parameters.put("l2ProcessorName", selectedProcessor.getExecutableName());
        parameters.put("l2ProcessorParameters", l2ProcessorPanel.getProcessorParameters());
        parameters.putAll(l3ParametersPanel.getValueMap());
        return parameters;
    }

    private class OrderProductionHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            String errorMessage = l3ParametersPanel.validate();
            if (errorMessage != null) {
                Window.alert(errorMessage);
                return;
            }

            GsProductionRequest request = getProductionRequest();
            getPortal().orderProduction(request);
        }

    }

}