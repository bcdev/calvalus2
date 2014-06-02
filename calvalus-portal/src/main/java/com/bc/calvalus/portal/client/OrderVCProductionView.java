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
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HTMLPanel;
import com.google.gwt.user.client.ui.HorizontalPanel;
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
    private static final String DIFFERENTIATION_SUFFIX = ".differentiation";

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private L2ConfigForm differentiationConfigForm;
    private L2ConfigForm l2ConfigForm;
    private MAConfigForm maConfigForm;
    private final CheckBox vcOutputL1;
    private final CheckBox vcOutputL1Diff;
    private final CheckBox vcOutputL2;
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

        differentiationConfigForm = new L2ConfigForm(portalContext, new DifferentiationFilter(), true);
        differentiationConfigForm.processorListLabel.setText("Differentiation Processor");
        differentiationConfigForm.parametersLabel.setText("Differentiation Parameters");

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

        ///////////
        HTMLPanel htmlPanel = new HTMLPanel("<h3>Vicarious-Calibration Output Parameters</h3><hr/>");
        htmlPanel.setWidth("62em");

        vcOutputL1 = new CheckBox("Output L1 Products");
        vcOutputL1Diff = new CheckBox("Output L1 Differentiation Products");
        vcOutputL2 = new CheckBox("Output L2 Products");

        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.setSpacing(4);
        verticalPanel.add(htmlPanel);
        verticalPanel.add(vcOutputL1);
        verticalPanel.add(vcOutputL1Diff);
        verticalPanel.add(vcOutputL2);

        HorizontalPanel vcPanel = new HorizontalPanel();
        vcPanel.setSpacing(16);
        vcPanel.add(verticalPanel);
        ///////////

        outputParametersForm = new OutputParametersForm();
        outputParametersForm.showFormatSelectionPanel(false);
        outputParametersForm.setAvailableOutputFormats("Report");

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(differentiationConfigForm);
        panel.add(l2ConfigForm);
        panel.add(maConfigForm);
        panel.add(vcPanel);
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
            differentiationConfigForm.validateForm();
            l2ConfigForm.validateForm();
            maConfigForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    public Map<String, String> getDifferentiationValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        DtoProcessorDescriptor processorDescriptor = differentiationConfigForm.getSelectedProcessorDescriptor();
        if (processorDescriptor != null) {
            parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME + DIFFERENTIATION_SUFFIX, processorDescriptor.getBundleName());
            parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION + DIFFERENTIATION_SUFFIX, processorDescriptor.getBundleVersion());
            parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION + DIFFERENTIATION_SUFFIX, processorDescriptor.getBundleLocation());
            parameters.put(ProcessorProductionRequest.PROCESSOR_NAME + DIFFERENTIATION_SUFFIX, processorDescriptor.getExecutableName());
            parameters.put(ProcessorProductionRequest.PROCESSOR_PARAMETERS + DIFFERENTIATION_SUFFIX, differentiationConfigForm.getProcessorParameters());
        }
        parameters.put("calvalus.vc.outputL1", vcOutputL1.getValue().toString());
        parameters.put("calvalus.vc.outputL1Diff", vcOutputL1Diff.getValue().toString());
        parameters.put("calvalus.vc.outputL2", vcOutputL2.getValue().toString());
        return parameters;
    }

    @Override
    protected HashMap<String, String> getProductionParameters() {
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.putAll(productSetSelectionForm.getValueMap());
        parameters.putAll(getDifferentiationValueMap());
        parameters.putAll(l2ConfigForm.getValueMap());
        parameters.putAll(maConfigForm.getValueMap());
        parameters.putAll(productSetFilterForm.getValueMap());
        parameters.putAll(outputParametersForm.getValueMap());
        parameters.put("autoStaging", "true");
        return parameters;
    }

    private static class DifferentiationFilter implements Filter<DtoProcessorDescriptor> {
        @Override
        public boolean accept(DtoProcessorDescriptor dtoProcessorDescriptor) {
            return dtoProcessorDescriptor.getProcessorCategory() == DtoProcessorDescriptor.DtoProcessorCategory.DIFFERENTIATION;
        }
    }
}