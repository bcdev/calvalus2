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
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.production.hadoop.ProcessorProductionRequest;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new Vicarious-Calibration production.
 *
 * @author Norman
 */
public class OrderVCProductionView extends OrderProductionView {
    public static final String ID = OrderVCProductionView.class.getName();
    private static final String PROCESSOR_PREFIX = "perturbation.";

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private L2ConfigForm perturbationConfigForm;
    private L2ConfigForm l2ConfigForm;
    private MAConfigForm maConfigForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderVCProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
            }
        });

        perturbationConfigForm = new L2ConfigForm(portalContext, new PerturbationFilter(), true);
        perturbationConfigForm.processorListLabel.setText("Perturbation Processor");
        perturbationConfigForm.parametersLabel.setText("Perturbation Parameters");

        l2ConfigForm = new L2ConfigForm(portalContext, true);
        l2ConfigForm.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                maConfigForm.setProcessorDescriptor(l2ConfigForm.getSelectedProcessorDescriptor());
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getProductSet());

        maConfigForm = new MAConfigForm(portalContext);
        maConfigForm.setProcessorDescriptor(l2ConfigForm.getSelectedProcessorDescriptor());

        outputParametersForm = new OutputParametersForm();
        outputParametersForm.showFormatSelectionPanel(false);
        outputParametersForm.setAvailableOutputFormats("Report");

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(perturbationConfigForm);
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
        return "Vicarious Calibration";
    }

    @Override
    protected String getProductionType() {
        return "VC";
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
            perturbationConfigForm.validateForm();
            l2ConfigForm.validateForm();
            maConfigForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    public Map<String, String> getPerturbationValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        DtoProcessorDescriptor processorDescriptor = perturbationConfigForm.getSelectedProcessorDescriptor();
        if (processorDescriptor != null) {
            parameters.put(PROCESSOR_PREFIX + ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, processorDescriptor.getBundleName());
            parameters.put(PROCESSOR_PREFIX + ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, processorDescriptor.getBundleVersion());
            parameters.put(PROCESSOR_PREFIX + ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, processorDescriptor.getBundleLocation());
            parameters.put(PROCESSOR_PREFIX + ProcessorProductionRequest.PROCESSOR_NAME, processorDescriptor.getExecutableName());
            parameters.put(PROCESSOR_PREFIX + ProcessorProductionRequest.PROCESSOR_PARAMETERS, perturbationConfigForm.getProcessorParameters());
        }
        return parameters;
    }

    @Override
    protected HashMap<String, String> getProductionParameters() {
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.putAll(productSetSelectionForm.getValueMap());
        parameters.putAll(getPerturbationValueMap());
        parameters.putAll(l2ConfigForm.getValueMap());
        parameters.putAll(maConfigForm.getValueMap());
        parameters.putAll(productSetFilterForm.getValueMap());
        parameters.putAll(outputParametersForm.getValueMap());
        parameters.put("autoStaging", "true");
        return parameters;
    }

    private static class PerturbationFilter implements Filter<DtoProcessorDescriptor> {
        @Override
        public boolean accept(DtoProcessorDescriptor dtoProcessorDescriptor) {
            return dtoProcessorDescriptor.getProcessorCategory() == DtoProcessorDescriptor.DtoProcessorCategory.PERTURBATION;
        }
    }
}