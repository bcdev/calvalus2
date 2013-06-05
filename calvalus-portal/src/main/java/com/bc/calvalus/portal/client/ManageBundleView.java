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

import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.Widget;

/**
 * Uploading (and maybe more...) for processor bundles
 */
public class ManageBundleView extends PortalView {

    public static final String ID = ManageBundleView.class.getName();

    private ManageBundleForm manageBundleForm;

    public ManageBundleView(PortalContext portalContext) {
        super(portalContext);
        manageBundleForm = new ManageBundleForm(portalContext);

        BundleUploadAction action = new BundleUploadAction();
        FileUploadManager.configureForm(manageBundleForm.getUploadForm(), "dir=" + "FOOOOO", action, action);
    }

    @Override
    public Widget asWidget() {
        return manageBundleForm;
    }

    @Override
    public String getViewId() {
        return ID;
    }

    @Override
    public String getTitle() {
        return "Manage Bundles";
    }

    private class BundleUploadAction implements FormPanel.SubmitCompleteHandler, FormPanel.SubmitHandler {
        @Override
        public void onSubmit(FormPanel.SubmitEvent event) {
            //TODO
        }

        @Override
        public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
            //TODO
        }
    }
}
