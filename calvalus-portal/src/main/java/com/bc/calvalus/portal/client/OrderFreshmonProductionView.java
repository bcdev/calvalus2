/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Demo view that lets users submit a new Freshmon production.
 */
public class OrderFreshmonProductionView extends OrderProductionView {

    public static final String ID = OrderFreshmonProductionView.class.getName();

    private static final String[] DEFAULT_BAND_SELECTION = new String[]{"CHL", "CDM", "TSM", "KDS"};

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;
    private L2ConfigForm l2ConfigForm;
    private OutputParametersForm outputParametersForm;
    private Widget widget;

    public OrderFreshmonProductionView(PortalContext portalContext) {
        super(portalContext);

        Filter<DtoProductSet> productSetFilter = new Filter<DtoProductSet>() {
            @Override
            public boolean accept(DtoProductSet dtoProductSet) {
                return dtoProductSet.getName().equals("MERIS FSG L1b 2002-2012") ||
                       dtoProductSet.getProductType().equals("FRESHMON_L2");
            }
        };
        Filter<DtoProcessorDescriptor> processorFilter = new Filter<DtoProcessorDescriptor>() {
            @Override
            public boolean accept(DtoProcessorDescriptor processorDescriptor) {
                if (processorDescriptor.getBundleName().startsWith("freshmon")) {
                    DtoProductSet productSet = productSetSelectionForm.getProductSet();
                    String productType = productSet.getProductType();
                    if (productType != null) {
                        String[] inputProductTypes = processorDescriptor.getInputProductTypes();
                        for (String inputProductType : inputProductTypes) {
                            if (inputProductType.equals(productType)) {
                                return true;
                            }
                        }
                    }
                }
                return false;
            }
        };
        final List<String> bandsToSelect = Arrays.asList(DEFAULT_BAND_SELECTION);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal(), productSetFilter);
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
                l2ConfigForm.updateProcessorList();
            }
        });

        l2ConfigForm = new L2ConfigForm(portalContext, processorFilter, true);
        l2ConfigForm.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                DtoProcessorDescriptor processorDescriptor = l2ConfigForm.getSelectedProcessorDescriptor();
                handleProcessorChanged(processorDescriptor, bandsToSelect);
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getProductSet());

        outputParametersForm = new OutputParametersForm();
        outputParametersForm.showFormatSelectionPanel(true);
        outputParametersForm.showTailoringRelatedSettings(true);
        outputParametersForm.quicklooks.setValue(true);
        outputParametersForm.replaceNans.setValue(true);
        outputParametersForm.replaceNanValue.setValue(0.0);
        handleProcessorChanged(l2ConfigForm.getSelectedProcessorDescriptor(), bandsToSelect);


        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);
        panel.add(l2ConfigForm);
        panel.add(outputParametersForm);
        panel.add(new HTML("<br/>"));
        panel.add(createOrderPanel());

        this.widget = panel;
    }

    private void handleProcessorChanged(DtoProcessorDescriptor processorDescriptor, List<String> bandsToSelect) {
        ListBox bandList = outputParametersForm.bandListBox;
        bandList.clear();
        if (processorDescriptor != null) {
            outputParametersForm.showFormatSelectionPanel(processorDescriptor.getFormattingType().equals("OPTIONAL"));
            outputParametersForm.setAvailableOutputFormats(processorDescriptor.getOutputFormats());
            int index = 0;
            for (DtoProcessorVariable variable : processorDescriptor.getProcessorVariables()) {
                bandList.addItem(variable.getName());
                if (bandsToSelect.contains(variable.getName())) {
                    bandList.setItemSelected(index, true);
                }
                index++;
            }
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
        return "Freshmon";
    }

    @Override
    protected String getProductionType() {
        return "L2Plus";
    }

    @Override
    public void onShowing() {
        // See http://code.google.com/p/gwt-google-apis/issues/detail?id=127
        productSetFilterForm.getRegionMap().getMapWidget().checkResizeAndCenter();
    }

    @Override
    protected boolean validateForm() {
        try {
            productSetSelectionForm.validateForm();
            productSetFilterForm.validateForm();
            l2ConfigForm.validateForm();
            outputParametersForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    @Override
    protected HashMap<String, String> getProductionParameters() {
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.putAll(productSetSelectionForm.getValueMap());
        parameters.putAll(productSetFilterForm.getValueMap());
        parameters.putAll(l2ConfigForm.getValueMap());
        parameters.putAll(outputParametersForm.getValueMap());
        parameters.put("additionalStagingPaths", "freshmon@freshmon-csw:/data/dissemination/entries/");
        return parameters;
    }
}