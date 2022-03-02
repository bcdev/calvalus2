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

import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.DtoCalvalusConfig;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.production.hadoop.ProcessorProductionRequest;
import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.*;

public class OrderCubegenProductionView extends OrderProductionView {

    public static final String ID = OrderCubegenProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private OutputParametersForm outputParametersForm;
    private Widget widget;
    private DtoProcessorDescriptor processorDescriptor;
    private String processorParameters;

    public OrderCubegenProductionView(PortalContext portalContext) {
        super(portalContext);

        Filter<DtoProductSet> productSetFilter = dtoProductSet ->
                dtoProductSet.getProductType() != null && dtoProductSet.getProductType().startsWith("L3");

        productSetSelectionForm = new ProductSetSelectionForm(getPortal(), productSetFilter, true);
        outputParametersForm = new OutputParametersForm(portalContext);
        outputParametersForm.setAvailableOutputFormats("XCube");
        outputParametersForm.autoStaging.setVisible(false);
        outputParametersForm.processingFormatCluster.setVisible(false);

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(outputParametersForm);
        panel.add(createOrderPanel());

        this.widget = panel;
        final BackendServiceAsync backendService = portalContext.getBackendService();
        backendService.getCalvalusConfig(new AsyncCallback<DtoCalvalusConfig>() {
            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error in retrieving calvalus config", caught.getMessage());
            }

            @Override
            public void onSuccess(DtoCalvalusConfig result) {
                String processorName = result.getConfig().get("calvalus.cubegen.processor.name");
                String processorVersion = result.getConfig().get("calvalus.cubegen.processor.version");
                processorParameters = result.getConfig().get("calvalus.cubegen.processor.parameters");
                backendService.getProcessors("processor=" + processorName + "," + processorVersion, new AsyncCallback<DtoProcessorDescriptor[]>() {
                    @Override
                    public void onFailure(Throwable caught) {
                        Dialog.error("Error in retrieving processors", caught.getMessage());
                    }

                    @Override
                    public void onSuccess(DtoProcessorDescriptor[] result) {
                        processorDescriptor = result[0];
                    }
                });
            }
        });
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
        return "Cube Generation";
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
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, processorDescriptor.getBundleName());
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, processorDescriptor.getBundleVersion());
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, processorDescriptor.getBundleLocation());
        parameters.put(ProcessorProductionRequest.PROCESSOR_NAME, processorDescriptor.getExecutableName());
        parameters.put(ProcessorProductionRequest.PROCESSOR_PARAMETERS, processorParameters);
        parameters.put(ProcessorProductionRequest.PROCESSOR_DESCRIPTION, processorDescriptor.getDescriptionHtml());
        parameters.put(ProcessorProductionRequest.OUTPUT_PRODUCT_TYPE, processorDescriptor.getOutputProductType());
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