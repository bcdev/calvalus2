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
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new Match-Up production.
 *
 * @author Norman
 */
public class OrderMAProductionView extends OrderProductionView {
    public static final String ID = OrderMAProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private ProductSelectionForm productSelectionForm;
    private L2ConfigForm l2ConfigForm;
    private MAConfigForm maConfigForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderMAProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
                l2ConfigForm.setProductSet(productSet);
            }
        });

        l2ConfigForm = new L2ConfigForm(portalContext, false);

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getSelectedProductSet());

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

        maConfigForm = new MAConfigForm(portalContext);

        outputParametersForm = new OutputParametersForm(portalContext);
        outputParametersForm.showFormatSelectionPanel(false);
        outputParametersForm.setAvailableOutputFormats("Report");

        l2ConfigForm.setProductSet(productSetSelectionForm.getSelectedProductSet());

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(productSelectionForm);
        panel.add(l2ConfigForm);
        panel.add(maConfigForm);
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
        return "Match-up Analysis";
    }

    @Override
    protected String getProductionType() {
        return "MA";
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
            maConfigForm.validateForm();
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
        parameters.putAll(maConfigForm.getValueMap());
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
        maConfigForm.setValues(parameters);
        outputParametersForm.setValues(parameters);
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
}