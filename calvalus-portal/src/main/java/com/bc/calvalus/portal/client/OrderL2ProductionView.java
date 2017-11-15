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
import com.google.gwt.core.client.Scheduler;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.*;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class OrderL2ProductionView extends OrderProductionView {

    public static final String ID = OrderL2ProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private ProductSelectionForm productSelectionForm;
    private L2ConfigForm l2ConfigForm;
    private OutputParametersForm outputParametersForm;
    private Widget widget;

    public OrderL2ProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(productSet -> {
            productSetFilterForm.setProductSet(productSet);
            l2ConfigForm.setProductSet(productSet);
        });

        l2ConfigForm = new L2ConfigForm(portalContext, false);
        l2ConfigForm.addChangeHandler(event -> handleProcessorChanged());

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getSelectedProductSet());

        productSelectionForm = new ProductSelectionForm(getPortal());
        productSelectionForm.addInputSelectionChangeHandler(productSelectionForm.getInputSelectionCallback());
        productSelectionForm.addInputSelectionChangeHandler(productSetSelectionForm.getInputSelectionCallback());
        productSelectionForm.addInputSelectionChangeHandler(productSetFilterForm.getInputSelectionCallback());

        outputParametersForm = new OutputParametersForm(portalContext);
        l2ConfigForm.setProductSet(productSetSelectionForm.getSelectedProductSet());
        handleProcessorChanged();

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(productSelectionForm);
        panel.add(l2ConfigForm);
        panel.add(outputParametersForm);
        Anchor l2Help = new Anchor("Show Help");
        l2Help.getElement().getStyle().setProperty("textDecoration", "none");
        l2Help.addStyleName("anchor");
        panel.add(l2Help);
        HelpSystem.addClickHandler(l2Help, "l2Processing");
        //panel.add(new HTML("<br/>"));
        panel.add(createOrderPanel());

        this.widget = panel;
    }

    private void handleProcessorChanged() {
        DtoProcessorDescriptor processorDescriptor = l2ConfigForm.getSelectedProcessorDescriptor();
        if (processorDescriptor != null) {
            outputParametersForm.showFormatSelectionPanel(processorDescriptor.getFormattingType().equals("OPTIONAL"));
            String[] processorOutputFormats = processorDescriptor.getOutputFormats();
            List<String> outputFormats = new ArrayList<>(Arrays.asList(processorOutputFormats));
            String formattingType = processorDescriptor.getFormattingType();
            boolean implicitlyFormatted = formattingType.equals("IMPLICIT");
            if (!implicitlyFormatted) {
                add("NetCDF4", outputFormats);
                add("BigGeoTiff", outputFormats);
            }
            outputParametersForm.setAvailableOutputFormats(outputFormats.toArray(new String[0]));
        }
    }

    private static void add(String format, List<String> outputFormats) {
        if (!outputFormats.contains(format)) {
            outputFormats.add(format);
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
        return "L2 Processing";
    }

    @Override
    protected String getProductionType() {
        return "L2Plus";
    }

    @Override
    public void onShowing() {
        // make sure #triggerResize is called after the new view is shown
        Scheduler.get().scheduleFinally(() -> {
            // See http://code.google.com/p/gwt-google-apis/issues/detail?id=127
            productSetFilterForm.getRegionMap().getMapWidget().triggerResize();
        });
    }

    @Override
    protected boolean validateForm() {
        try {
            productSetSelectionForm.validateForm();
            productSetFilterForm.validateForm();
            l2ConfigForm.validateForm();
            outputParametersForm.validateForm();
            if (!getPortal().withPortalFeature("unlimitedJobSize")) {
                try {
                    final int numDaysValue = Integer.parseInt(productSetFilterForm.numDays.getValue());
                    if (numDaysValue > 365 + 366) {
                        throw new ValidationException(productSetFilterForm.numDays, "time range larger than allowed");
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

    @Override
    protected HashMap<String, String> getProductionParameters() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.putAll(productSetSelectionForm.getValueMap());
        parameters.putAll(productSetFilterForm.getValueMap());
        parameters.putAll(l2ConfigForm.getValueMap());
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
        outputParametersForm.setValues(parameters);
    }
}