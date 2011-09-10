package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L3 production.
 *
 * @author Norman
 */
public class OrderMAProductionView extends OrderProductionView {
    public static final String ID = OrderMAProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private ProcessorSelectionForm processorSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private ProcessorParametersForm processorParametersForm;
    private MAConfigForm maConfigForm;

    private Widget widget;

    public OrderMAProductionView(PortalContext portalContext) {
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
                maConfigForm.setSelectedProcessor(processorDescriptor);
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getSelectedProductSet());
        productSetFilterForm.addChangeHandler(new ProductSetFilterForm.ChangeHandler() {
            @Override
            public void temporalFilterChanged(Map<String, String> data) {
                // ?
            }

            @Override
            public void spatialFilterChanged(Map<String, String> data) {
                // ?
            }
        });

        processorParametersForm = new ProcessorParametersForm("Processing Parameters");
        processorParametersForm.setProcessorDescriptor(processorSelectionForm.getSelectedProcessor());

        maConfigForm = new MAConfigForm(portalContext);
        maConfigForm.setSelectedProcessor(processorSelectionForm.getSelectedProcessor());

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
        panel.add(maConfigForm);
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
        return "Match-up Analysis";
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
            maConfigForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    @Override
    protected DtoProductionRequest getProductionRequest() {
        return new DtoProductionRequest("MA", getProductionParameters());
    }

    // todo - Provide JUnit test for this method
    public HashMap<String, String> getProductionParameters() {
        DtoProcessorDescriptor selectedProcessor = processorSelectionForm.getSelectedProcessor();
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputPath", productSetSelectionForm.getSelectedProductSet().getPath());
        parameters.put("autoStaging", "true");
        parameters.put("processorBundleName", selectedProcessor.getBundleName());
        parameters.put("processorBundleVersion", selectedProcessor.getBundleVersion());
        parameters.put("processorName", selectedProcessor.getExecutableName());
        parameters.put("processorParameters", processorParametersForm.getProcessorParameters());
        parameters.putAll(maConfigForm.getValueMap());
        parameters.putAll(productSetFilterForm.getValueMap());
        return parameters;
    }
}