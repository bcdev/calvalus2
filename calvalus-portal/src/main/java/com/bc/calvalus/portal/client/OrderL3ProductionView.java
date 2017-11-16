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

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.core.client.Scheduler;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Anchor;
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
    private ProductSelectionForm productSelectionForm;
    private L2ConfigForm l2ConfigForm;
    private L3ConfigForm l3ConfigForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderL3ProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
                l3ConfigForm.setProcessorDescriptor(l2ConfigForm.getSelectedProcessorDescriptor(), productSetSelectionForm.getSelectedProductSet());
                l2ConfigForm.setProductSet(productSet);
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getSelectedProductSet());
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

        productSelectionForm = new ProductSelectionForm(getPortal());
        productSelectionForm.addInputSelectionHandler(new ProductSelectionForm.ClickHandler() {
            @Override
            public AsyncCallback<DtoInputSelection> getInputSelectionChangedCallback() {
                return new InputSelectionCallback();
            }

            @Override
            public void onClearSelectionClick() {
                productSelectionForm.removeSelections();
                productSetSelectionForm.removeSelections();
                productSetFilterForm.removeSelections();
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
        l3ConfigForm.steppingPeriodLength.setValue(30);
        l3ConfigForm.compositingPeriodLength.setValue(30);

        outputParametersForm = new OutputParametersForm(portalContext);
        outputParametersForm.setAvailableOutputFormats("BEAM-DIMAP", "NetCDF", "NetCDF4", "GeoTIFF", "BigGeoTiff");

        l2ConfigForm.setProductSet(productSetSelectionForm.getSelectedProductSet());
        updateTemporalParameters(productSetFilterForm.getValueMap());

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(productSelectionForm);
        panel.add(l2ConfigForm);
        panel.add(l3ConfigForm);
        panel.add(outputParametersForm);
        //panel.add(new HTML("<br/>"));
        Anchor l3Help = new Anchor("Show Help");
        panel.add(l3Help);
        HelpSystem.addClickHandler(l3Help, "l3Processing");
        panel.add(createOrderPanel());

        this.widget = panel;
    }

    private class InputSelectionCallback implements AsyncCallback<DtoInputSelection> {

        @Override
        public void onSuccess(DtoInputSelection inputSelection) {
            Map<String, String> inputSelectionMap = OrderL2ProductionView.parseParametersFromContext(inputSelection);
            productSelectionForm.setValues(inputSelectionMap);
            productSetSelectionForm.setValues(inputSelectionMap);
            productSetFilterForm.setValues(inputSelectionMap);
        }

        @Override
        public void onFailure(Throwable caught) {
            Dialog.error("Error in retrieving input selection", caught.getMessage());
        }
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
    protected String getProductionType() {
        return "L3";
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
            if (! getPortal().withPortalFeature("unlimitedJobSize")) {
                try {
                    final int numPeriods = l3ConfigForm.periodCount.getValue();
                    final int periodLength = l3ConfigForm.compositingPeriodLength.getValue();
                    if (numPeriods * periodLength > 365+366) {
                        throw new ValidationException(productSetFilterForm.numDays, "days to be processed larger than allowed");
                    }
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
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
        l3ConfigForm.setValues(parameters);
        outputParametersForm.setValues(parameters);
    }
}