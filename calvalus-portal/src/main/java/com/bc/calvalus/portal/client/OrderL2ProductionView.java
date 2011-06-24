package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProcessorDescriptor;
import com.bc.calvalus.portal.shared.GsProductionRequest;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class OrderL2ProductionView extends OrderProductionView {
    public static final String ID = OrderL2ProductionView.class.getName();
    private ProductSetSelectionForm productSetSelectionForm;
    private OutputParametersForm outputParametersForm;
    private GeneralProcessorForm processorForm;
    private ProductFilterForm productSetFilterForm;
    private Widget widget;

    public OrderL2ProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal().getProductSets());
        productSetFilterForm = new ProductFilterForm(portalContext.getRegions(), new ProductFilterForm.ChangeHandler() {
            @Override
            public void dateChanged(Map<String, String> data) {

            }

            @Override
            public void regionChanged(Map<String, String> data) {
            }
        });
        outputParametersForm = new OutputParametersForm();
        processorForm = new GeneralProcessorForm(getPortal().getProcessors(), "L2 Processor");

        HorizontalPanel hpanel = new HorizontalPanel();
        hpanel.setHorizontalAlignment(HasHorizontalAlignment.ALIGN_RIGHT);
        hpanel.add(new Button("Order Production", new ClickHandler() {
            public void onClick(ClickEvent event) {
                orderProduction();
            }
        }));

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(processorForm);
        panel.add(outputParametersForm);
        panel.add(hpanel);

        this.widget = panel;
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
        return "L2 Processing";
    }

    @Override
    protected boolean validateForm() {
        try {
            productSetFilterForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    @Override
    protected GsProductionRequest getProductionRequest() {
        return new GsProductionRequest("L2", getProductionParameters());
    }

    private HashMap<String, String> getProductionParameters() {
        GsProcessorDescriptor selectedProcessor = processorForm.getSelectedProcessor();
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputProductSetId", productSetSelectionForm.getInputProductSetId());
        parameters.put("outputFormat", outputParametersForm.getOutputFormat());
        parameters.put("autoStaging", outputParametersForm.isAutoStaging() + "");
        parameters.put("processorBundleName", selectedProcessor.getBundleName());
        parameters.put("processorBundleVersion", selectedProcessor.getBundleVersion());
        parameters.put("processorName", selectedProcessor.getExecutableName());
        parameters.put("processorParameters", processorForm.getProcessorParameters());
        parameters.putAll(productSetFilterForm.getValueMap());
        return parameters;
    }
}