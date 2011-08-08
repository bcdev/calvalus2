package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.*;

import java.util.Date;
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

        productSetSelectionForm = new ProductSetSelectionForm(getPortal().getProductSets());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
            }
        });

        processorSelectionForm = new ProcessorSelectionForm(portalContext.getProcessors(), "Processor");
        processorSelectionForm.addChangeHandler(new ProcessorSelectionForm.ChangeHandler() {
            @Override
            public void onProcessorChanged(DtoProcessorDescriptor processorDescriptor) {
                processorParametersForm.setProcessorDescriptor(processorDescriptor);
                binningParametersForm.setSelectedProcessor(processorDescriptor);
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getSelectedProductSet());
        productSetFilterForm.temporalFilterByDateRange.setValue(false);
        productSetFilterForm.temporalFilterOff.setValue(true, true);
        productSetFilterForm.temporalFilterByDateList.setEnabled(false);
        productSetFilterForm.addChangeHandler(new ProductSetFilterForm.ChangeHandler() {
            @Override
            public void temporalFilterChanged(Map<String, String> data) {
                updateTemporalParameters(data);
            }

            @Override
            public void spatialFilterChanged(Map<String, String> data) {
                binningParametersForm.updateSpatialParameters(productSetFilterForm.getSelectedRegion());
            }
        });

        processorParametersForm = new ProcessorParametersForm("Processing Parameters");
        processorParametersForm.setProcessorDescriptor(processorSelectionForm.getSelectedProcessor());

        binningParametersForm = new BinningParametersForm();
        binningParametersForm.setSelectedProcessor(processorSelectionForm.getSelectedProcessor());
        binningParametersForm.resolution.setEnabled(false);
        binningParametersForm.superSampling.setEnabled(false);
        binningParametersForm.steppingPeriodLength.setValue(32);
        binningParametersForm.compositingPeriodLength.setValue(4);

        updateTemporalParameters(productSetFilterForm.getValueMap());

        outputParametersForm = new OutputParametersForm();

        Button orderButton = new Button("Order Production");
        orderButton.addClickHandler(new ClickHandler() {
            public void onClick(ClickEvent event) {
                orderProduction();
            }
        });

        Button checkButton = new Button("Check Request");
        checkButton.addClickHandler(new ClickHandler() {
            public void onClick(ClickEvent event) {
                checkRequest();
            }
        });

        HorizontalPanel buttonPanel = new HorizontalPanel();
        buttonPanel.setSpacing(2);
        buttonPanel.add(checkButton);
        buttonPanel.add(orderButton);

        HorizontalPanel orderPanel = new HorizontalPanel();
        orderPanel.setWidth("100%");
        orderPanel.add(buttonPanel);
        orderPanel.setCellHorizontalAlignment(buttonPanel, HasHorizontalAlignment.ALIGN_RIGHT);

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

    private void updateTemporalParameters(Map<String, String> data) {
        String minDateString = data.get("minDate");
        String maxDateString = data.get("maxDate");
        Date minDate = null;
        Date maxDate = null;
        if (minDateString != null && maxDateString != null) {
            minDate = ProductSetFilterForm.DATE_FORMAT.parse(minDateString);
            maxDate = ProductSetFilterForm.DATE_FORMAT.parse(maxDateString);
        }
        binningParametersForm.updateTemporalParameters(minDate, maxDate);
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
            processorSelectionForm.validateForm();
            productSetFilterForm.validateForm();
            processorParametersForm.validateForm();
            binningParametersForm.validateForm();
            //outputParametersForm.validateForm();
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
        parameters.put("inputPath", productSetSelectionForm.getSelectedProductSet().getPath());
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