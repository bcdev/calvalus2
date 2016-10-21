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
import com.google.gwt.core.client.Scheduler;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2-to-L3 production.
 *
 * @author Norman
 */
public class OrderL2toL3ProductionView extends OrderProductionView {

    public static final String ID = OrderL2toL3ProductionView.class.getName();
    private final CheckBox outputMeanL3Products;

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private L2ConfigForm l2ConfigForm;
    private L3ConfigForm l3ConfigForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderL2toL3ProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
                l2ConfigForm.setProductSet(productSet);
                l2ConfigForm.updateProcessorList();
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getSelectedProductSet());
        productSetFilterForm.temporalFilterByDateList.setValue(false);
        productSetFilterForm.temporalFilterOff.setValue(false, true);
        productSetFilterForm.temporalFilterOff.setEnabled(false);
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

        l3ConfigForm = new L3ConfigForm(portalContext, "AVG");
        l3ConfigForm.setProcessorDescriptor(l2ConfigForm.getSelectedProcessorDescriptor(), productSetSelectionForm.getSelectedProductSet());
        l3ConfigForm.resolution.setEnabled(false);
        l3ConfigForm.superSampling.setEnabled(false);
        l3ConfigForm.steppingPeriodLength.setValue(15);
        l3ConfigForm.compositingPeriodLength.setValue(15);
        l3ConfigForm.compositingType.setEnabled(false);

        updateTemporalParameters(productSetFilterForm.getValueMap());

        outputParametersForm = new OutputParametersForm();
        outputMeanL3Products = new CheckBox("Output mean Level-3 products.");
        outputMeanL3Products.setValue(false);
        outputParametersForm.productRelatedPanel.add(outputMeanL3Products);
        outputParametersForm.showFormatSelectionPanel(false);
        outputParametersForm.setAvailableOutputFormats("Report");

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(l2ConfigForm);
        panel.add(l3ConfigForm);
        panel.add(outputParametersForm);
        //panel.add(new HTML("<br/>"));
        Anchor l3Help = new Anchor("Show Help");
        panel.add(l3Help);
        HelpSystem.addClickHandler(l3Help, "l2tol3Processing");
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
        return "L2 to L3 Comparison";
    }

    @Override
    protected String getProductionType() {
        return "L2toL3";
    }

    @Override
    public void onShowing() {
        // make sure #triggerResize is called after the new view is shown
        Scheduler.get().scheduleFinally(new Scheduler.ScheduledCommand() {
            @Override
            public void execute() {
                // See http://code.google.com/p/gwt-google-apis/issues/detail?id=127
                productSetFilterForm.getRegionMap().getMapWidget().triggerResize();
            }
        });
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
        parameters.put("outputMeanL3", outputMeanL3Products.getValue().toString());
        parameters.put("autoStaging", "true");
        return parameters;
    }
}