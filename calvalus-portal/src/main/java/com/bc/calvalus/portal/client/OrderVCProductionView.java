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
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HTMLPanel;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.Panel;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * View that lets users submit a new Vicarious-Calibration production.
 *
 * @author MarcoZ
 */
public class OrderVCProductionView extends OrderProductionView {
    public static final String ID = OrderVCProductionView.class.getName();
    private static final String DIFFERENTIATION_SUFFIX = ".differentiation";

    private final ProductSetSelectionForm productSetSelectionForm;
    private final ProductSetFilterForm productSetFilterForm;
    private final L2ConfigForm differentiationConfigForm;
    private final L2ConfigForm l2ConfigForm;
    private final MAConfigForm maConfigForm;
    private final CheckBox vcOutputL1;
    private final CheckBox vcOutputL1Diff;
    private final CheckBox vcOutputL2;
    private final OutputParametersForm outputParametersForm;
    private final TextBox goodPixelExpression;
    private final TextBox goodRecordExpression;

    private final Widget widget;

    public OrderVCProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
                l2ConfigForm.setProductSet(productSet);
                l2ConfigForm.updateProcessorList();
            }
        });

        differentiationConfigForm = new L2ConfigForm(portalContext, new DifferentiationFilter(), true);
        differentiationConfigForm.processorListLabel.setText("Differentiation Processor");
        differentiationConfigForm.parametersLabel.setText("Differentiation Parameters");

        l2ConfigForm = new L2ConfigForm(portalContext, true);

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        productSetFilterForm.setProductSet(productSetSelectionForm.getSelectedProductSet());

        maConfigForm = new MAConfigForm(portalContext);
        maConfigForm.expressionTable.setVisible(false);

        ///////////
        HTMLPanel htmlPanel = new HTMLPanel("<h3>Vicarious-Calibration Output Parameters</h3><hr/>");
        htmlPanel.setWidth("62em");

        goodPixelExpression = new TextBox();
        goodRecordExpression = new TextBox();
        goodPixelExpression.setWidth("36em");
        goodRecordExpression.setWidth("36em");

        Panel pixelPanel = new HorizontalPanel();
        pixelPanel.add(new Label("Un-differentiated Level 2 Good-pixel expression:"));
        pixelPanel.add(goodPixelExpression);

        Panel recordPanel = new HorizontalPanel();
        recordPanel.add(new Label("Un-differentiated Level 2 Good-record expression:"));
        recordPanel.add(goodRecordExpression);

        vcOutputL1 = new CheckBox("Output L1 Products");
        vcOutputL1Diff = new CheckBox("Output L1 Differentiation Products");
        vcOutputL2 = new CheckBox("Output L2 Products");

        VerticalPanel verticalPanel = new VerticalPanel();
        verticalPanel.setSpacing(4);
        verticalPanel.add(htmlPanel);
        verticalPanel.add(pixelPanel);
        verticalPanel.add(recordPanel);
        verticalPanel.add(new HTML("<br/>"));
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
        outputParametersForm.allowedFailure.setValue(5);

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
        parameters.putAll(differentiationConfigForm.getValueMap(DIFFERENTIATION_SUFFIX));
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
        // overwrite with local values
        parameters.put("goodPixelExpression", goodPixelExpression.getText());
        parameters.put("goodRecordExpression", goodRecordExpression.getText());

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