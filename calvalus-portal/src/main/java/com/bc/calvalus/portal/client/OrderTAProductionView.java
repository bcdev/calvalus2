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
 * Demo view that lets users submit a new production of a trend-analysis report.
 *
 * @author Norman
 */
public class OrderTAProductionView extends OrderProductionView {
    public static final String ID = OrderTAProductionView.class.getName();
    private FlexTable widget;
    private InputOutputForm inputOutputForm;
    private GeneralProcessorForm l2ProcessorForm;
    private L3ParametersForm l3ParametersForm;

    public OrderTAProductionView(PortalContext portalContext) {
        super(portalContext);

        inputOutputForm = new InputOutputForm(getPortal().getProductSets(), "L1 Input", false);
        l2ProcessorForm = new GeneralProcessorForm(getPortal().getProcessors(), "L2 Processor");
        l3ParametersForm = new L3ParametersForm(portalContext.getRegions());

        widget = new FlexTable();
        widget.getFlexCellFormatter().setVerticalAlignment(0, 0, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setVerticalAlignment(0, 1, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setVerticalAlignment(1, 0, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setVerticalAlignment(1, 1, HasVerticalAlignment.ALIGN_TOP);
        widget.getFlexCellFormatter().setHorizontalAlignment(2, 0, HasHorizontalAlignment.ALIGN_RIGHT);
        widget.getFlexCellFormatter().setColSpan(2, 0, 2);
        widget.getFlexCellFormatter().setRowSpan(0, 1, 2);
        widget.ensureDebugId("widget");
        widget.setWidth("32em");
        widget.setCellSpacing(2);
        widget.setCellPadding(2);
        widget.setWidget(0, 0, inputOutputForm.asWidget());
        widget.setWidget(1, 0, l2ProcessorForm.asWidget());
        widget.setWidget(0, 1, l3ParametersForm.asWidget());
        widget.setWidget(2, 0, new Button("Order Production", new ClickHandler() {
            public void onClick(ClickEvent event) {
                orderProduction();
            }
        }));
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
        return "L3 Trend Analysis";
    }

    @Override
    protected boolean validateForm() {
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

    @Override
    protected GsProductionRequest getProductionRequest() {
        return new GsProductionRequest("TA", getProductionParameters());
    }

    private HashMap<String, String> getProductionParameters() {
        GsProcessorDescriptor selectedProcessor = l2ProcessorForm.getSelectedProcessor();
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputProductSetId", inputOutputForm.getInputProductSetId());
        parameters.put("outputFormat", inputOutputForm.getOutputFormat());
        parameters.put("autoStaging", inputOutputForm.isAutoStaging() + "");
        parameters.put("processorBundleName", selectedProcessor.getBundleName());
        parameters.put("processorBundleVersion", selectedProcessor.getBundleVersion());
        parameters.put("processorName", selectedProcessor.getExecutableName());
        parameters.put("processorParameters", l2ProcessorForm.getProcessorParameters());
        parameters.putAll(l3ParametersForm.getValueMap());
        return parameters;
    }
}