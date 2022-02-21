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
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.bc.calvalus.production.hadoop.ProcessorProductionRequest;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.*;

public class OrderCubegenProductionView extends OrderProductionView {

    public static final String ID = OrderCubegenProductionView.class.getName();

    private ProductSetSelectionForm productSetSelectionForm;
    private OutputParametersForm outputParametersForm;
    private Widget widget;
    private final String[] processorConfig;

    public OrderCubegenProductionView(PortalContext portalContext) {
        super(portalContext);

        Filter<DtoProductSet> productSetFilter = new Filter<DtoProductSet>() {
            @Override
            public boolean accept(DtoProductSet dtoProductSet) {
                return dtoProductSet.getProductType() != null && dtoProductSet.getProductType().startsWith("L3");
            }
        };

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

        processorConfig = new String[7];

        this.widget = panel;
        final BackendServiceAsync backendService = portalContext.getBackendService();
        backendService.getCalvalusConfig(new AsyncCallback<DtoCalvalusConfig>() {
            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error in retrieving calvalus config", caught.getMessage());
            }

            @Override
            public void onSuccess(DtoCalvalusConfig result) {
                String bundleName = result.getConfig().get("calvalus.cubegen.processor.bundleName");
                String bundleVersion = result.getConfig().get("calvalus.cubegen.processor.bundleVersion");
                String bundleLocation = result.getConfig().get("calvalus.cubegen.processor.bundleLocation");
                String processorName = result.getConfig().get("calvalus.cubegen.processor.name");
                String processorDescription = result.getConfig().get("calvalus.cubegen.processor.description");
                String processorParameters = result.getConfig().get("calvalus.cubegen.processor.parameters");
                String processorOutputType = result.getConfig().get("calvalus.cubegen.processor.outputType");
                processorConfig[0] = bundleName;
                processorConfig[1] = bundleVersion;
                processorConfig[2] = bundleLocation;
                processorConfig[3] = processorName;
                processorConfig[4] = processorDescription;
                processorConfig[5] = processorParameters;
                processorConfig[6] = processorOutputType;
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
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_NAME, processorConfig[0]);
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_VERSION, processorConfig[1]);
        parameters.put(ProcessorProductionRequest.PROCESSOR_BUNDLE_LOCATION, processorConfig[2]);
        parameters.put(ProcessorProductionRequest.PROCESSOR_NAME, processorConfig[3]);
        parameters.put(ProcessorProductionRequest.PROCESSOR_DESCRIPTION, processorConfig[4]);
        parameters.put(ProcessorProductionRequest.PROCESSOR_PARAMETERS, processorConfig[5]);
        parameters.put(ProcessorProductionRequest.OUTPUT_PRODUCT_TYPE, processorConfig[6]);
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