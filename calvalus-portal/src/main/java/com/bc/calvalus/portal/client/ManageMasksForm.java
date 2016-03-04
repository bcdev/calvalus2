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

import com.bc.calvalus.portal.shared.DtoMaskDescriptor;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Form for handling masks (and maybe more...)
 */
public class ManageMasksForm extends Composite {

    private static final String MASK_DIRECTORY = "masks";

    private final PortalContext portalContext;

    interface TheUiBinder extends UiBinder<Widget, ManageMasksForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    CalvalusStyle style;

    @UiField
    ListBox maskList;
    @UiField
    Button removeButton;
    @UiField
    Button uploadButton;
    @UiField
    Anchor showHelp;

    private DtoMaskDescriptor[] maskDescriptors;

    public ManageMasksForm(PortalContext portalContext) {
        this.portalContext = portalContext;
        initWidget(uiBinder.createAndBindUi(this));

        updateMasksList();
        removeButton.addClickHandler(new MaskRemoveHandler());
        uploadButton.addClickHandler(new MaskUploadHandler());
        HelpSystem.addClickHandler(showHelp, "managingMasks");
    }

    private void updateMasksList() {
        portalContext.getBackendService().getMasks(new AsyncCallback<DtoMaskDescriptor[]>() {
            @Override
            public void onFailure(Throwable caught) {
                // error is handled elsewhere
            }

            @Override
            public void onSuccess(DtoMaskDescriptor[] result) {
                maskDescriptors = result;
                fillMaskList();
            }
        });
    }

    private void fillMaskList() {
        maskList.clear();
        if (maskDescriptors.length > 0) {
            Set<String> masks = new HashSet<String>(maskDescriptors.length);
            for (DtoMaskDescriptor descriptor : maskDescriptors) {
                masks.add(descriptor.getMaskName());
            }
            ArrayList<String> maskNames = new ArrayList<String>(masks);
            Collections.sort(maskNames);
            for (String maskName : maskNames) {
                maskList.addItem(maskName);
            }
            maskList.setSelectedIndex(0);
        }
    }

    private class MaskRemoveHandler implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            final int selectedIndex = maskList.getSelectedIndex();
            final String maskName;
            if (selectedIndex >= 0) {
                maskName = maskList.getItemText(selectedIndex);
            } else {
                maskName = null;
            }
            if (maskName != null) {
                Dialog.ask("Remove Mask",
                        new HTML("The mask '" + maskName + "' will be permanently deleted.<br/>" +
                                "Do you really want to continue?"),
                        new Runnable() {
                            @Override
                            public void run() {
                                MaskRemoveHandler.this.removeMask(maskName);
                                ManageMasksForm.this.updateMasksList();
                            }
                        });
            } else {
                Dialog.error("Remove Mask",
                        "No mask selected.");
            }

        }

        private void removeMask(final String maskName) {
            portalContext.getBackendService().removeUserFile(MASK_DIRECTORY + "/" + maskName, new AsyncCallback<Boolean>() {
                @Override
                public void onFailure(Throwable caught) {
                    Dialog.error("Remove Mask",
                            "No mask selected.");
                }

                @Override
                public void onSuccess(Boolean result) {
                    Dialog.info("Remove Mask",
                            "Mask '" + maskName + "' has successfully been removed.");
                }
            });
        }
    }

    private class MaskUploadHandler implements ClickHandler {

        private final FileUpload maskFileUpload;
        private final FormPanel uploadForm;
        private Dialog fileUploadDialog;
        private Dialog monitorDialog;
        private FormPanel.SubmitEvent submitEvent;

        private MaskUploadHandler() {
            maskFileUpload = new FileUpload();
            maskFileUpload.setName("maskUpload");
            uploadForm = new FormPanel();
            uploadForm.setWidget(maskFileUpload);

            final MaskSubmitHandler submitHandler = new MaskSubmitHandler();
            FileUploadManager.configureForm(uploadForm,
                    "dir=" + MASK_DIRECTORY + "&mask=true",
                    submitHandler,
                    submitHandler);
        }

        @Override
        public void onClick(ClickEvent event) {
            VerticalPanel verticalPanel = UIUtils.createVerticalPanel(2,
                    new HTML("Select a mask file:"),
                    uploadForm,
                    new HTML("The mask file must be a SNAP-readable, one-banded data product.<br/>" +
                            "Uploads will replace existing masks if they have the same name."));
            fileUploadDialog = new Dialog("Mask Upload", verticalPanel, Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
                @Override
                protected void onOk() {
                    String filename = fileNameOf(maskFileUpload.getFilename());
                    if (filename == null || filename.isEmpty()) {
                        Dialog.error("Mask Upload",
                                new HTML("No filename selected."),
                                new HTML("Please specify a mask file."));
                        return;
                    }
                    monitorDialog = new Dialog("Mask Upload", new Label("Sending '" + filename + "'..."), ButtonType.CANCEL) {
                        @Override
                        protected void onCancel() {
                            cancelSubmit();
                        }
                    };
                    monitorDialog.show();
                    uploadForm.submit();
                }

                private void cancelSubmit() {
                    closeDialogs();
                    if (submitEvent != null) {
                        submitEvent.cancel();
                    }
                }
            };

            fileUploadDialog.show();
        }

        private void closeDialogs() {
            monitorDialog.hide();
            fileUploadDialog.hide();
        }

        private String fileNameOf(String path) {
            if (path == null) {
                return null;
            }
            path = path.substring(path.lastIndexOf('/') + 1);
            path = path.substring(path.lastIndexOf('\\') + 1);
            return path;
        }

        private class MaskSubmitHandler implements FormPanel.SubmitHandler, FormPanel.SubmitCompleteHandler {

            @Override
            public void onSubmit(FormPanel.SubmitEvent submitEvent) {
                MaskUploadHandler.this.submitEvent = submitEvent;
            }

            @Override
            public void onSubmitComplete(FormPanel.SubmitCompleteEvent submitCompleteEvent) {
                closeDialogs();
                updateMasksList();
                String resultHTML = submitCompleteEvent.getResults();
                if (resultHTML.equals(fileNameOf(maskFileUpload.getFilename()))) {
                    Dialog.info("Mask Upload", "File successfully uploaded.");
                } else {
                    Dialog.error("Mask Upload", "Error during mask upload.<br/>" + resultHTML);
                }
            }
        }
    }
}
