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

import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistical bootstrapping for data sets
 */
public class OrderBootstrappingView extends OrderProductionView {


    private final VerticalPanel widget;
    private final BootstrappingForm bootstrappingForm;

    public OrderBootstrappingView(PortalContext portalContext) {
        super(portalContext);

        this.bootstrappingForm = new BootstrappingForm(portalContext);
        VerticalPanel panel = new VerticalPanel();
        panel.setWidth("100%");
        panel.add(bootstrappingForm);
        panel.add(new HTML("<br/>"));
        panel.add(createOrderPanel());

        this.widget = panel;

    }

    @Override
    protected String getProductionType() {
        return "Bootstrapping";
    }

    @Override
    protected Map<String, String> getProductionParameters() {
        HashMap<String, String> parameters = new HashMap<String, String>();
        parameters.putAll(bootstrappingForm.getValueMap());
        return parameters;
    }

    @Override
    protected boolean validateForm() {
        try {
            bootstrappingForm.validateForm();
            return true;
        } catch (ValidationException e) {
            e.handle();
            return false;
        }
    }

    @Override
    public Widget asWidget() {
        return widget;
    }

    @Override
    public String getTitle() {
        return "Bootstrapping";
    }

}
