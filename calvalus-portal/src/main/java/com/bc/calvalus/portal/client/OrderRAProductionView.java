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

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.core.client.Scheduler;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new Region analysis production.
 *
 * @author MarcoZ
 */
public class OrderRAProductionView extends OrderProductionView {
    public static final String ID = OrderRAProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private L2ConfigForm l2ConfigForm;
    private RAConfigForm raConfigForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderRAProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
                l2ConfigForm.setProductSet(productSet);
                raConfigForm.setProcessorDescriptor(l2ConfigForm.getSelectedProcessorDescriptor(), productSetSelectionForm.getSelectedProductSet());
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.temporalFilterByDateRange.setValue(false);
        productSetFilterForm.temporalFilterOff.setValue(true, true);
        productSetFilterForm.temporalFilterByDateList.setEnabled(false);

        productSetFilterForm.spatialFilterByRegion.setValue(true, true);
        productSetFilterForm.spatialFilterOff.setEnabled(false);
        productSetFilterForm.setProductSet(productSetSelectionForm.getSelectedProductSet());
        productSetFilterForm.addChangeHandler(new ProductSetFilterForm.ChangeHandler() {
            @Override
            public void temporalFilterChanged(Map<String, String> data) {
                updateTemporalParameters(data);
            }

            @Override
            public void spatialFilterChanged(Map<String, String> data) {
            }
        });

        l2ConfigForm = new L2ConfigForm(portalContext, false);
        l2ConfigForm.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                raConfigForm.setProcessorDescriptor(l2ConfigForm.getSelectedProcessorDescriptor(), productSetSelectionForm.getSelectedProductSet());
            }
        });


        raConfigForm = new RAConfigForm(portalContext);
        raConfigForm.setProcessorDescriptor(l2ConfigForm.getSelectedProcessorDescriptor(), productSetSelectionForm.getSelectedProductSet());
        
        outputParametersForm = new OutputParametersForm(portalContext);
        outputParametersForm.showFormatSelectionPanel(false);
        outputParametersForm.setAvailableOutputFormats("Report");

        l2ConfigForm.setProductSet(productSetSelectionForm.getSelectedProductSet());
        updateTemporalParameters(productSetFilterForm.getValueMap());

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(l2ConfigForm);
        panel.add(raConfigForm);
        panel.add(outputParametersForm);
        panel.add(new HTML("<br/>"));
        panel.add(createOrderPanel());

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
        return "Regional Statistics";
    }

    @Override
    protected String getProductionType() {
        return "RA";
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
            raConfigForm.validateForm();
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
        parameters.putAll(raConfigForm.getValueMap());
        parameters.putAll(outputParametersForm.getValueMap());
        parameters.put("autoStaging", "true");
        return parameters;
    }

    @Override
    public boolean isRestoringRequestPossible() {
        return true;
    }

    @Override
    public void setProductionParameters(Map<String, String> parameters) {
        productSetSelectionForm.setValues(parameters);
        productSetFilterForm.setValues(parameters);
        l2ConfigForm.setValues(parameters);
        raConfigForm.setValues(parameters);
        outputParametersForm.setValues(parameters);
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
        raConfigForm.updateTemporalParameters(minDate, maxDate);
    }

//    private void updateBandList(DtoProcessorDescriptor processorDescriptor) {
//        ListBox bandList = raConfigForm.bandListBox;
//        bandList.clear();
//        if (processorDescriptor != null) {
//            int index = 0;
//            for (DtoProcessorVariable variable : processorDescriptor.getProcessorVariables()) {
//                bandList.addItem(variable.getName());
//                bandList.setItemSelected(index, true);
//                index++;
//            }
//        }
//    }
//
//    private void updateBandList(DtoProductSet productSet) {
//        ListBox bandList = raConfigForm.bandListBox;
//        bandList.clear();
//        if (productSet != null) {
//            int index = 0;
//            for (String variable : productSet.getBandNames()) {
//                bandList.addItem(variable);
//                bandList.setItemSelected(index, true);
//                index++;
//            }
//        }
//    }

}