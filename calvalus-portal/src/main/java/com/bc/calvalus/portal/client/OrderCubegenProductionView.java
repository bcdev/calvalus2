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

import com.bc.calvalus.production.hadoop.ProcessorProductionRequest;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.*;

public class OrderCubegenProductionView extends OrderProductionView {

    public static final String ID = OrderCubegenProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private OutputParametersForm outputParametersForm;
    private Widget widget;

    public OrderCubegenProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal(), null, true);
        outputParametersForm = new OutputParametersForm(portalContext);
        outputParametersForm.setAvailableOutputFormats(new String[]{"XCube"});
        outputParametersForm.autoStaging.setVisible(false);
        outputParametersForm.processingFormatCluster.setVisible(false);

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(outputParametersForm);
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
        return "Cubegen Processing";
    }

    @Override
    protected String getProductionType() {
        return "L2Plus";
    }

    @Override
    protected boolean validateForm() {
        try {
            productSetSelectionForm.validateForm();
            outputParametersForm.validateForm();
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
        return true;
    }

    @Override
    protected HashMap<String, String> getProductionParameters() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.putAll(productSetSelectionForm.getValueMap());
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, "cubegen");
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, "1.1");
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, "/calvalus/projects/bigfe/software/cubegen-1.1");
        parameters.put(ProcessorProductionRequest.PROCESSOR_NAME, "XCube");
        parameters.put(ProcessorProductionRequest.PROCESSOR_DESCRIPTION, "Cube generation");
        parameters.put(ProcessorProductionRequest.PROCESSOR_PARAMETERS, "");
        parameters.put(ProcessorProductionRequest.OUTPUT_PRODUCT_TYPE, "XCube_S2_L2_C2RCC");
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
        outputParametersForm.setValues(parameters);
    }

}