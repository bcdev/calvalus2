package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new production of a trend-analysis report.
 *
 * @author Norman
 */
public class OrderTAProductionView extends OrderProductionView {
    public static final String ID = OrderTAProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private ProcessorSelectionForm processorSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private ProcessorParametersForm processorParametersForm;
    private BinningParametersForm binningParametersForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderTAProductionView(PortalContext portalContext) {
        super(portalContext);

        Button orderButton = new Button("Order Production");
        Button checkButton = new Button("Check Request");
        productSetSelectionForm = new ProductSetSelectionForm(getPortal().getProductSets());
        productSetFilterForm = new ProductSetFilterForm(portalContext);
        processorSelectionForm = new ProcessorSelectionForm(portalContext.getProcessors(), "Processor");
        processorParametersForm = new ProcessorParametersForm("Processing Parameters");
        binningParametersForm = new BinningParametersForm();
        outputParametersForm = new OutputParametersForm();

        orderButton.addClickHandler(new ClickHandler() {
            public void onClick(ClickEvent event) {
                orderProduction();
            }
        });
        checkButton.addClickHandler(new ClickHandler() {
            public void onClick(ClickEvent event) {
                checkRequest();
            }
        });

        productSetFilterForm.addChangeHandler(new ProductSetFilterForm.ChangeHandler() {
            @Override
            public void dateChanged(Map<String, String> data) {
                binningParametersForm.updateTemporalParameters(productSetFilterForm.getMinDate(),
                                                               productSetFilterForm.getMaxDate());
            }

            @Override
            public void regionChanged(Map<String, String> data) {
                binningParametersForm.updateSpatialParameters(productSetFilterForm.getSelectedRegion());
            }
        });

        processorSelectionForm.addChangeHandler(new ProcessorSelectionForm.ChangeHandler() {
            @Override
            public void onProcessorChanged(DtoProcessorDescriptor processorDescriptor) {
                processorParametersForm.setProcessorDescriptor(processorDescriptor);
                binningParametersForm.setSelectedProcessor(processorDescriptor);
            }
        });
        processorParametersForm.setProcessorDescriptor(processorSelectionForm.getSelectedProcessor());
        binningParametersForm.setSelectedProcessor(processorSelectionForm.getSelectedProcessor());

        HorizontalPanel buttonPanel = new HorizontalPanel();
        buttonPanel.setSpacing(2);
        buttonPanel.add(checkButton);
        buttonPanel.add(orderButton);

        HorizontalPanel orderPanel = new HorizontalPanel();
        orderPanel.setCellHorizontalAlignment(buttonPanel, HasHorizontalAlignment.ALIGN_RIGHT);
        orderPanel.setWidth("100%");
        orderPanel.add(buttonPanel);

        HorizontalPanel panel1 = new HorizontalPanel();
        panel1.setSpacing(16);
        panel1.add(productSetSelectionForm);
        panel1.add(processorSelectionForm);

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(panel1);
        panel.add(productSetFilterForm);
        panel.add(processorParametersForm);
        panel.add(binningParametersForm);
        // panel.add(outputParametersForm);
        panel.add(new HTML("<br/>"));
        panel.add(orderPanel);

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
        return "Trend Analysis";
    }

    @Override
    public void onShowing() {
        // See http://code.google.com/p/gwt-google-apis/issues/detail?id=127
        productSetFilterForm.getRegionMap().getMapWidget().checkResizeAndCenter();
    }

    @Override
    protected boolean validateForm() {
        try {
            productSetSelectionForm.validateForm();
            productSetFilterForm.validateForm();
            processorSelectionForm.validateForm();
            binningParametersForm.validateForm();
            outputParametersForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    @Override
    protected DtoProductionRequest getProductionRequest() {
        return new DtoProductionRequest("TA", getProductionParameters());
    }

    // todo - Provide JUnit test for this method
    public HashMap<String, String> getProductionParameters() {
        DtoProcessorDescriptor selectedProcessor = processorSelectionForm.getSelectedProcessor();
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputProductSetId", productSetSelectionForm.getInputProductSetId());
        parameters.put("outputFormat", outputParametersForm.getOutputFormat());
        parameters.put("autoStaging", outputParametersForm.isAutoStaging() + "");
        parameters.put("processorBundleName", selectedProcessor.getBundleName());
        parameters.put("processorBundleVersion", selectedProcessor.getBundleVersion());
        parameters.put("processorName", selectedProcessor.getExecutableName());
        parameters.put("processorParameters", processorParametersForm.getProcessorParameters());
        parameters.putAll(binningParametersForm.getValueMap());
        parameters.putAll(productSetFilterForm.getValueMap());
        return parameters;
    }
}