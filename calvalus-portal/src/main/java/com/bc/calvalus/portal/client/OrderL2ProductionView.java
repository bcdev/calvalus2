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
    public static final String ID = OrderL2ProductionView.class.getName();
    private InputOutputForm inputOutputForm;
    private GeneralProcessorForm processingForm;
    private L2ProductFilterForm productFilterForm;
    private FlexTable widget;

    public OrderL2ProductionView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        inputOutputForm = new InputOutputForm(calvalusPortal, "L2 Input/Output");
        processingForm = new GeneralProcessorForm(getPortal(), "L2 Processor");
        productFilterForm = new L2ProductFilterForm();

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
        widget.setWidget(0, 0, inputOutputForm.asWidget());
        widget.setWidget(0, 1, processingForm.asWidget());
        widget.setWidget(1, 0, productFilterForm.asWidget());
        widget.setWidget(2, 0, new Button("Order Production", new OrderProductionHandler()));
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    @Override
    public String getViewId() {
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
        GsProcessorDescriptor selectedProcessor = processingForm.getSelectedProcessor();
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputProductSetId", inputOutputForm.getInputProductSetId());
        parameters.put("outputFormat", inputOutputForm.getOutputFormat());
        parameters.put("autoStaging", inputOutputForm.isAutoStaging() + "");
        parameters.put("processorBundleName", selectedProcessor.getBundleName());
        parameters.put("processorBundleVersion", processingForm.getBundleVersion());
        parameters.put("processorName", selectedProcessor.getExecutableName());
        parameters.put("processorParameters", processingForm.getProcessorParameters());
        parameters.putAll(productFilterForm.getValueMap());
        return parameters;
    }

    private boolean validateForm() {
        try {
            productFilterForm.validateForm(3);
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }


    private class OrderProductionHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            if (validateForm()) {
                GsProductionRequest request = getProductionRequest();
                getPortal().orderProduction(request);
            }
        }
    }

}