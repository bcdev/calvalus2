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
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Form for handling bundles (and maybe more...) for processor bundles
 */
public class ManageBundleForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, ManageBundleForm> {

    }
    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    FileUpload fileUpload;
    @UiField
    FormPanel uploadForm;
    @UiField
    ListBox bundleList;
    @UiField
    TextBox bundleName;
    @UiField
    Button submitButton;
    @UiField
    TextBox bundleVersion;
    @UiField
    Button removeButton;

    public ManageBundleForm(PortalContext portalContext) {
        initWidget(uiBinder.createAndBindUi(this));

        fillBundleList(portalContext.getProcessors());
        removeButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                Dialog.info("Warning", "You are not allowed to remove this bundle.");
            }
        });
    }

    private void fillBundleList(DtoProcessorDescriptor[] processorDescriptors) {
        Set<String> bundles = new HashSet<String>(processorDescriptors.length);
        for (DtoProcessorDescriptor descriptor : processorDescriptors) {
            bundles.add(descriptor.getBundleName() + "-" + descriptor.getBundleVersion());
        }
        ArrayList<String> bundleNames = new ArrayList<String>(bundles);
        Collections.sort(bundleNames);
        for (String bundle : bundleNames) {
            bundleList.addItem(bundle);
        }
    }

    public FormPanel getUploadForm() {
        return uploadForm;
    }

}
