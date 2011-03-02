package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProductionRequest;
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
    private L3ProcessorPanel l3ProcessorPanel;

    public OrderL3ProductionView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        inputOutputPanel = new InputOutputPanel(getPortal(), "L3 Input/Output");
        l2ProcessorPanel = new GeneralProcessorPanel(getPortal(), "L2 Processor");
        l3ProcessorPanel = new L3ProcessorPanel();

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
        widget.setWidget(0, 1, l3ProcessorPanel.asWidget());
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

    // todo - test
    public PortalProductionRequest getProductionRequest() {
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputProductSetId", inputOutputPanel.getInputProductSetId());
        parameters.put("outputFileName", inputOutputPanel.getOutputFileName());
        parameters.put("outputFormat", inputOutputPanel.getOutputFormat());
        parameters.put("outputStaging", inputOutputPanel.getOutputStaging() + "");
        parameters.put("l2OperatorName", l2ProcessorPanel.getProcessorId());
        parameters.put("l2OperatorParameters", l2ProcessorPanel.getProcessorParameters());
        parameters.putAll(l3ProcessorPanel.getParameterMap());
        return new PortalProductionRequest("calvalus-level3", parameters);
    }

    private class OrderProductionHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            String errorMessage = l3ProcessorPanel.validate();
            if (errorMessage != null) {
                Window.alert(errorMessage);
                return;
            }

            PortalProductionRequest request = getProductionRequest();

            getPortal().orderProduction(request);
        }

    }

}