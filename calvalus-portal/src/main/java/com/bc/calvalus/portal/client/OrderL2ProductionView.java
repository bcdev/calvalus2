package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProductionParameter;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.Widget;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class OrderL2ProductionView extends PortalView {
    public static final int ID = 1;
    private InputOutputPanel inputOutputPanel;
    private GeneralProcessorPanel processingPanel;
    private FlexTable widget;

    public OrderL2ProductionView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        inputOutputPanel = new InputOutputPanel(calvalusPortal, "L2 Input/Output");
        processingPanel = new GeneralProcessorPanel(getPortal(), "L2 Processor");

        widget = new FlexTable();
        widget.getFlexCellFormatter().setVerticalAlignment(0, 0, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setVerticalAlignment(0, 1, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setHorizontalAlignment(1, 0, HasHorizontalAlignment.ALIGN_RIGHT);
        widget.getFlexCellFormatter().setColSpan(1, 0, 2);
        widget.ensureDebugId("cwFlexTable");
        widget.addStyleName("cw-FlexTable");
        widget.setWidth("32em");
        widget.setCellSpacing(2);
        widget.setCellPadding(2);
        widget.setWidget(0, 0, inputOutputPanel.asWidget());
        widget.setWidget(0, 1, processingPanel.asWidget());
        widget.setWidget(1, 0, new Button("Order Production", new OrderProductionHandler()));
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

    private class OrderProductionHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            PortalProductionRequest request = new PortalProductionRequest("calvalus-level2",
                                                                          new PortalProductionParameter("inputProductSetId",
                                                                                              inputOutputPanel.getInputProductSetId()),
                                                                          new PortalProductionParameter("outputFileName",
                                                                                              inputOutputPanel.getOutputFileName()),
                                                                          new PortalProductionParameter("processorId",
                                                                                              processingPanel.getProcessorId()),
                                                                          new PortalProductionParameter("processorVersion",
                                                                                              processingPanel.getProcessorVersion()),
                                                                          new PortalProductionParameter("processorParameters",
                                                                                              processingPanel.getProcessorParameters()));
            getPortal().orderProduction(request);
        }


    }

}