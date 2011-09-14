package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Demo view that lets users submit a new L3 production.
 *
 * @author Norman
 */
public class OrderL3ProductionView extends OrderProductionView {
    public static final String ID = OrderL3ProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private L2ConfigForm l2ConfigForm;
    private L3ConfigForm l3ConfigForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderL3ProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal().getProductSets());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getProductSet());
        productSetFilterForm.temporalFilterOff.setEnabled(false);
        productSetFilterForm.addChangeHandler(new ProductSetFilterForm.ChangeHandler() {
            @Override
            public void temporalFilterChanged(Map<String, String> data) {
                updateTemporalParameters(data);
            }

            @Override
            public void spatialFilterChanged(Map<String, String> data) {
                l3ConfigForm.updateSpatialParameters(productSetFilterForm.getSelectedRegion());
            }
        });

        l2ConfigForm = new L2ConfigForm(portalContext, false);
        l2ConfigForm.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                l3ConfigForm.setProcessorDescriptor(l2ConfigForm.getProcessorDescriptor());
            }
        });

        l3ConfigForm = new L3ConfigForm();
        l3ConfigForm.setProcessorDescriptor(l2ConfigForm.getProcessorDescriptor());
        l3ConfigForm.steppingPeriodLength.setValue(30);
        l3ConfigForm.compositingPeriodLength.setValue(30);

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

        updateTemporalParameters(productSetFilterForm.getValueMap());

        HorizontalPanel buttonPanel = new HorizontalPanel();
        buttonPanel.setSpacing(2);
        buttonPanel.add(checkButton);
        buttonPanel.add(orderButton);

        HorizontalPanel orderPanel = new HorizontalPanel();
        orderPanel.setWidth("100%");
        orderPanel.add(buttonPanel);
        orderPanel.setCellHorizontalAlignment(buttonPanel, HasHorizontalAlignment.ALIGN_RIGHT);

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(l2ConfigForm);
        panel.add(l3ConfigForm);
        panel.add(outputParametersForm);
        panel.add(new HTML("<br/>"));
        panel.add(orderPanel);

        this.widget = panel;
    }

    private void updateTemporalParameters(Map<String, String> data) {
        boolean dateList = data.containsKey("dateList");
        if (dateList) {
            String[] splits = data.get("dateList").split("\\s");
            HashSet<String> set = new HashSet<String>(Arrays.asList(splits));
            set.remove("");
            int numDays = set.size();
            l3ConfigForm.periodCount.setValue(numDays);

            l3ConfigForm.steppingPeriodLength.setEnabled(false);
            l3ConfigForm.steppingPeriodLength.setValue(1);

            l3ConfigForm.compositingPeriodLength.setEnabled(false);
            l3ConfigForm.compositingPeriodLength.setValue(1);
        } else {
            l3ConfigForm.steppingPeriodLength.setEnabled(true);
            l3ConfigForm.compositingPeriodLength.setEnabled(true);

            String minDateString = data.get("minDate");
            String maxDateString = data.get("maxDate");
            Date minDate = null;
            Date maxDate = null;
            if (minDateString != null && maxDateString != null) {
                minDate = ProductSetFilterForm.DATE_FORMAT.parse(minDateString);
                maxDate = ProductSetFilterForm.DATE_FORMAT.parse(maxDateString);
            }
            l3ConfigForm.updateTemporalParameters(minDate, maxDate);
        }
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
        return "L3 Processing";
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
            l2ConfigForm.validateForm();
            l3ConfigForm.validateForm();
            outputParametersForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    @Override
    protected DtoProductionRequest getProductionRequest() {
        return new DtoProductionRequest("L3", getProductionParameters());
    }

    // todo - Provide JUnit test for this method
    public HashMap<String, String> getProductionParameters() {
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.putAll(productSetSelectionForm.getValueMap());
        parameters.putAll(productSetFilterForm.getValueMap());
        parameters.putAll(l2ConfigForm.getValueMap());
        parameters.putAll(l3ConfigForm.getValueMap());
        parameters.putAll(outputParametersForm.getValueMap());
        return parameters;
    }
}