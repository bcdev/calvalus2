package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalParameter;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
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
public class OrderL3ProductionView extends PortalView {
    public static final int ID = 2;
    private FlexTable widget;
    private InputOutputPanel inputOutputPanel;
    private GeneralProcessorPanel l2ProcessorPanel;
    private L3ProcessorPanel l3ProcessorPanel;

    public OrderL3ProductionView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        inputOutputPanel = new InputOutputPanel(calvalusPortal, "L3 Input/Output");
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
        widget.setWidget(2, 0, new Button("Order Production", new SubmitHandler()));
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

    private class SubmitHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            PortalProductionRequest request = new PortalProductionRequest("calvalus.level3",
                                                                          new PortalParameter("inputProductSetId",
                                                                                              inputOutputPanel.getInputProductSetId()),
                                                                          new PortalParameter("outputFileName",
                                                                                              inputOutputPanel.getOutputFileName()),
                                                                          new PortalParameter("l3ProcessorParameters",
                                                                                              l3ProcessorPanel.getProcessorParameters()),
                                                                          new PortalParameter("l2ProcessorId",
                                                                                              l2ProcessorPanel.getProcessorId()),
                                                                          new PortalParameter("l2ProcessorVersion",
                                                                                              l2ProcessorPanel.getProcessorVersion()),
                                                                          new PortalParameter("l2ProcessorParameters",
                                                                                              l2ProcessorPanel.getProcessorParameters())
            );
            getPortal().getBackendService().orderProduction(request, new AsyncCallback<PortalProductionResponse>() {
                public void onSuccess(final PortalProductionResponse response) {
                    ManageProductionsView view = (ManageProductionsView) getPortal().getView(ManageProductionsView.ID);
                    view.show();
                }

                public void onFailure(Throwable caught) {
                    Window.alert("Error!\n" + caught.getMessage());
                }
            });
        }

    }

}