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
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.google.gwt.core.client.Scheduler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class OrderQLProductionView extends OrderProductionView {

    public static final String ID = OrderQLProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private ProductsFromCatalogueForm productsFromCatalogueForm;
    private OutputParametersForm outputParametersForm;
    private QuicklookParametersForm quicklookParametersForm;
    private Widget widget;

    public OrderQLProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getSelectedProductSet());

        if (getPortal().withPortalFeature(INPUT_FILES_PANEL)) {
            productsFromCatalogueForm = new ProductsFromCatalogueForm(getPortal());
            productsFromCatalogueForm.addInputSelectionHandler(new ProductsFromCatalogueForm.InputSelectionHandler() {
                @Override
                public AsyncCallback<DtoInputSelection> getInputSelectionChangedCallback() {
                    return new InputSelectionCallback();
                }

                @Override
                public void onClearSelectionClick() {
                    productsFromCatalogueForm.removeSelections();
                }
            });
        }

        outputParametersForm = new OutputParametersForm(portalContext);
        outputParametersForm.showFormatSelectionPanel(false);
        outputParametersForm.setAvailableOutputFormats("Image");

        quicklookParametersForm = new QuicklookParametersForm(portalContext);
        quicklookParametersForm.setBandNames();

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        if (getPortal().withPortalFeature(INPUT_FILES_PANEL)){
            panel.add(productsFromCatalogueForm);
        }
        panel.add(outputParametersForm);
        panel.add(quicklookParametersForm);
        Anchor l2Help = new Anchor("Show Help");
        l2Help.getElement().getStyle().setProperty("textDecoration", "none");
        l2Help.addStyleName("anchor");
        panel.add(l2Help);
        HelpSystem.addClickHandler(l2Help, "l2Processing");
        //panel.add(new HTML("<br/>"));
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
        return "Quicklook generation";
    }

    @Override
    protected String getProductionType() {
        return "QL";
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
            if (productsFromCatalogueForm != null) {
                productsFromCatalogueForm.validateForm(productSetSelectionForm.getSelectedProductSet().getName());
            }
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
        if (productsFromCatalogueForm != null) {
            parameters.putAll(productsFromCatalogueForm.getValueMap());
        }
        parameters.putAll(outputParametersForm.getValueMap());
        parameters.putAll(quicklookParametersForm.getValueMap());
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
        if (productsFromCatalogueForm != null) {
            productsFromCatalogueForm.setValues(parameters);
        }
        outputParametersForm.setValues(parameters);
        quicklookParametersForm.setValues(parameters);
    }

    private class InputSelectionCallback implements AsyncCallback<DtoInputSelection> {

        @Override
        public void onSuccess(DtoInputSelection inputSelection) {
            Map<String, String> inputSelectionMap = UIUtils.parseParametersFromContext(inputSelection);
            productsFromCatalogueForm.setValues(inputSelectionMap);
            productSetSelectionForm.setValues(inputSelectionMap);
            productSetFilterForm.setValues(inputSelectionMap);
        }

        @Override
        public void onFailure(Throwable caught) {
            Dialog.error("Error in retrieving input selection", caught.getMessage());
        }
    }
}
