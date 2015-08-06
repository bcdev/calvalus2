/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

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
    private ProductSetFilterForm productSetFilterForm;
    private L2ConfigForm l2ConfigForm;
    private L3ConfigForm l3ConfigForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderTAProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
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
                l3ConfigForm.updateSpatialParameters(productSetFilterForm.getSelectedRegion());
            }
        });

        l2ConfigForm = new L2ConfigForm(portalContext, false);
        l2ConfigForm.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                l3ConfigForm.setProcessorDescriptor(l2ConfigForm.getSelectedProcessorDescriptor(), productSetSelectionForm.getSelectedProductSet());
            }
        });

        l3ConfigForm = new L3ConfigForm(portalContext);
        l3ConfigForm.setProcessorDescriptor(l2ConfigForm.getSelectedProcessorDescriptor(), productSetSelectionForm.getSelectedProductSet());
        l3ConfigForm.resolution.setEnabled(false);
        l3ConfigForm.superSampling.setEnabled(false);
        l3ConfigForm.steppingPeriodLength.setValue(32);
        l3ConfigForm.compositingPeriodLength.setValue(4);

        updateTemporalParameters(productSetFilterForm.getValueMap());

        outputParametersForm = new OutputParametersForm();
        outputParametersForm.showFormatSelectionPanel(false);
        outputParametersForm.setAvailableOutputFormats("Report");

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(l2ConfigForm);
        panel.add(l3ConfigForm);
        panel.add(outputParametersForm);
        panel.add(new HTML("<br/>"));
        panel.add(createOrderPanel());

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
        l3ConfigForm.updateTemporalParameters(minDate, maxDate);
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
    protected String getProductionType() {
        return "TA";
    }

    @Override
    public void onShowing() {
        // See http://code.google.com/p/gwt-google-apis/issues/detail?id=127
        productSetFilterForm.getRegionMap().getMapWidget().triggerResize();
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

    // todo - Provide JUnit test for this method
    @Override
    protected HashMap<String, String> getProductionParameters() {
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.putAll(productSetSelectionForm.getValueMap());
        parameters.putAll(productSetFilterForm.getValueMap());
        parameters.putAll(l2ConfigForm.getValueMap());
        parameters.putAll(l3ConfigForm.getValueMap());
        parameters.putAll(outputParametersForm.getValueMap());
        parameters.put("autoStaging", "true");
        return parameters;
    }
}