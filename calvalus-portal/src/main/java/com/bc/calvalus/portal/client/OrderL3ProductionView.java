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
 * Demo view that lets users submit a new L3 production.
 *
 * @author Norman
 */
public class OrderL3ProductionView extends PortalView {
    public static final String ID = OrderL3ProductionView.class.getName();
    private FlexTable widget;
    private InputOutputForm inputOutputForm;
    private GeneralProcessorForm l2ProcessorForm;
    private L3ParametersForm l3ParametersForm;

    public OrderL3ProductionView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        inputOutputForm = new InputOutputForm(getPortal(), "L1 Input / L3 Output", true);
        l2ProcessorForm = new GeneralProcessorForm(getPortal(), "L2 Processor");
        l3ParametersForm = new L3ParametersForm();

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
        widget.setWidget(0, 0, inputOutputForm.asWidget());
        widget.setWidget(1, 0, l2ProcessorForm.asWidget());
        widget.setWidget(0, 1, l3ParametersForm.asWidget());
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
        return "Level 3";
    }

    private boolean validateView() {
        try {
            inputOutputForm.validateForm();
            l2ProcessorForm.validateForm();
            l3ParametersForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    // todo - Provide JUnit test for this method
    public GsProductionRequest getProductionRequest() {
        return new GsProductionRequest("L3", getValueMap());
    }

    // todo - Provide JUnit test for this method
    public HashMap<String, String> getValueMap() {
        GsProcessorDescriptor selectedProcessor = l2ProcessorForm.getSelectedProcessor();
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputProductSetId", inputOutputForm.getInputProductSetId());
        parameters.put("outputFormat", inputOutputForm.getOutputFormat());
        parameters.put("autoStaging", inputOutputForm.isAutoStaging() + "");
        parameters.put("processorBundleName", selectedProcessor.getBundleName());
        parameters.put("processorBundleVersion", l2ProcessorForm.getBundleVersion());
        parameters.put("processorName", selectedProcessor.getExecutableName());
        parameters.put("processorParameters", l2ProcessorForm.getProcessorParameters());
        parameters.putAll(l3ParametersForm.getValueMap());
        return parameters;
    }

    private class OrderProductionHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            if (validateView()) {
                GsProductionRequest request = getProductionRequest();
                getPortal().orderProduction(request);
            }
        }

    }

}