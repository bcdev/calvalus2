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

import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.core.client.Scheduler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * View that lets users submit a new match-up-comparison production.
 *
 * @author MarcoZ
 */
public class OrderMACProductionView extends OrderProductionView {
    public static final String ID = OrderMACProductionView.class.getName();
    private static final int NUM_PROCESSORS = 6;

    private ProductSetSelectionForm productSetSelectionForm;
    private ProductSetFilterForm productSetFilterForm;

    private L2MaConfigForm[] l2MaForms;
    private MAConfigForm maConfigForm;
    private OutputParametersForm outputParametersForm;

    private Widget widget;

    public OrderMACProductionView(PortalContext portalContext) {
        super(portalContext);

        productSetSelectionForm = new ProductSetSelectionForm(getPortal());
        productSetSelectionForm.addChangeHandler(new ProductSetSelectionForm.ChangeHandler() {
            @Override
            public void onProductSetChanged(DtoProductSet productSet) {
                productSetFilterForm.setProductSet(productSet);
                for (int i = 0; i < l2MaForms.length; i++) {
                    l2MaForms[i].l2ConfigForm.setProductSet(productSet);
                }
            }
        });

        productSetFilterForm = new ProductSetFilterForm(portalContext);
        DtoProductSet selectedProductSet = productSetSelectionForm.getSelectedProductSet();
        productSetFilterForm.setProductSet(selectedProductSet);

        l2MaForms = new L2MaConfigForm[NUM_PROCESSORS];
        boolean selectionMandatory = false;
        for (int i = 0; i < l2MaForms.length; i++) {
            l2MaForms[i] = new L2MaConfigForm(portalContext, selectionMandatory);
            l2MaForms[i].l2ConfigForm.setProductSet(selectedProductSet);
            selectionMandatory = true;
            if (i >= 2) {
                l2MaForms[i].enabledCheckbox.setValue(false, true);
            }
        }

        maConfigForm = new MAConfigForm(portalContext);
        maConfigForm.expressionTable.setVisible(false);

        outputParametersForm = new OutputParametersForm(portalContext);
        outputParametersForm.showFormatSelectionPanel(false);
        outputParametersForm.setAvailableOutputFormats("Report");

        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(productSetSelectionForm);
        panel.add(productSetFilterForm);

        VerticalPanel l2MultiPanel = new VerticalPanel();
        l2MultiPanel.setSpacing(4);
        l2MultiPanel.add(new HTML("<h3>Multiple Level-2 Processors for comparison</h3><hr/>"));
        for (L2MaConfigForm l2MaForm : l2MaForms) {
            l2MultiPanel.add(l2MaForm);
            l2MultiPanel.add(new HTML("<hr/>"));
        }
        panel.add(l2MultiPanel);

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
        return "Match-up Comparison";
    }

    @Override
    protected String getProductionType() {
        return "MAC";
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
            List<String> allIdentifiers = new ArrayList<String>(l2MaForms.length);
            for (L2MaConfigForm l2MaForm : l2MaForms) {
                if (l2MaForm.enabledCheckbox.getValue()) {
                    String identifier = l2MaForm.getProcessorIdentifierSafe();
                    if (allIdentifiers.contains(identifier)) {
                        throw new ValidationException(l2MaForm.processorIdentifier, "The processor identifiers must be unique.");
                    }
                    allIdentifiers.add(identifier);
                }
            }
            if (allIdentifiers.size() < 2) {
                throw new ValidationException(l2MaForms[0], "At least 2 processors must be configured for comparison.");
            }
            for (L2MaConfigForm l2MaForm : l2MaForms) {
                l2MaForm.validateForm();
            }
            maConfigForm.validateForm();
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

        String allIdentifiers = "";
        for (L2MaConfigForm form : l2MaForms) {
            if (form.enabledCheckbox.getValue()) {
                String identifier = form.getProcessorIdentifierSafe();
                String suffix = "." + identifier;
                parameters.putAll(form.l2ConfigForm.getValueMap(suffix));
                parameters.put("goodPixelExpression" + suffix, form.goodPixelExpression.getText());
                parameters.put("goodRecordExpression" + suffix, form.goodRecordExpression.getText());
                if (allIdentifiers.isEmpty()) {
                    allIdentifiers = identifier;
                } else {
                    allIdentifiers = allIdentifiers + "," + identifier;
                }
            }
        }

        parameters.put("allIdentifiers", allIdentifiers);
        parameters.putAll(maConfigForm.getValueMap());
        parameters.remove("goodPixelExpression");
        parameters.remove("goodRecordExpression");
        parameters.putAll(productSetFilterForm.getValueMap());
        parameters.putAll(outputParametersForm.getValueMap());
        parameters.put("autoStaging", "true");
        return parameters;
    }

    private static class L2MaConfigForm extends Composite {
        private final CheckBox enabledCheckbox;
        private final TextBox processorIdentifier;
        private final L2ConfigForm l2ConfigForm;
        private final TextBox goodPixelExpression;
        private final TextBox goodRecordExpression;

        private L2MaConfigForm(PortalContext portalContext, boolean selectionMandatory) {

            enabledCheckbox = new CheckBox("Enabled");
            enabledCheckbox.setValue(true);
            enabledCheckbox.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
                @Override
                public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
                    Boolean isEnabled = booleanValueChangeEvent.getValue();
                    processorIdentifier.setEnabled(isEnabled);
                    l2ConfigForm.processorList.setEnabled(isEnabled);
                    l2ConfigForm.processorParametersArea.setEnabled(isEnabled);
                    l2ConfigForm.fileUpload.setEnabled(isEnabled);
                    goodPixelExpression.setEnabled(isEnabled);
                    goodRecordExpression.setEnabled(isEnabled);
                }
            });

            processorIdentifier = new TextBox();
            l2ConfigForm = new L2ConfigForm(portalContext, selectionMandatory);
            goodPixelExpression = new TextBox();
            goodRecordExpression = new TextBox();

            processorIdentifier.setWidth("6em");
            goodPixelExpression.setWidth("36em");
            goodRecordExpression.setWidth("36em");

            HorizontalPanel identifierPanel = new HorizontalPanel();
            identifierPanel.add(new Label("Identifier: "));
            identifierPanel.add(processorIdentifier);

            HorizontalPanel expressionPanel = new HorizontalPanel();
            VerticalPanel labelPanel = new VerticalPanel();
            labelPanel.setSpacing(4);
            labelPanel.add(new Label("Good-pixel expression:"));
            labelPanel.add(new Label("Good-record expression:"));
            VerticalPanel textboxPanel = new VerticalPanel();
            textboxPanel.setSpacing(4);
            textboxPanel.add(goodPixelExpression);
            textboxPanel.add(goodRecordExpression);
            expressionPanel.add(labelPanel);
            expressionPanel.add(textboxPanel);

            VerticalPanel verticalPanel = new VerticalPanel();
            verticalPanel.setSpacing(4);
            verticalPanel.add(enabledCheckbox);
            verticalPanel.add(identifierPanel);
            verticalPanel.add(l2ConfigForm);
            verticalPanel.add(expressionPanel);

            initWidget(verticalPanel);
        }

        public void validateForm() throws ValidationException {
            if (enabledCheckbox.getValue()) {
                if (processorIdentifier.getValue().isEmpty()) {
                    throw new ValidationException(processorIdentifier, "A processor identifier must be given.");
                }
                l2ConfigForm.validateForm();
            }
        }

        public String getProcessorIdentifierSafe() {
            String value = processorIdentifier.getValue().trim();
            value = value.replaceAll("\\s+",""); // whitepace
            value = value.replaceAll("<","_"); // XML
            value = value.replaceAll(">","_"); // XML
            value = value.replaceAll("&","_"); // XML
            return value;
        }
    }
}