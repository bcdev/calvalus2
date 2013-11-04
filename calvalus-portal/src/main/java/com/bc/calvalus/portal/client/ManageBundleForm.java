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

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FlexTable;
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
 * Form for handling bundles (and maybe more...) for processor bundles
 */
public class ManageBundleForm extends Composite {

    private static final String BUNDLE_DIRECTORY = "software";

    private final PortalContext portalContext;

    interface TheUiBinder extends UiBinder<Widget, ManageBundleForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    L2ConfigForm.L2Style style;

    @UiField
    ListBox bundleList;
    @UiField
    Button removeButton;
    @UiField
    Button uploadButton;
    @UiField
    Anchor showHelp;

    @UiField
    FlexTable processors;

    private DtoProcessorDescriptor[] processorsDesc;

    public ManageBundleForm(PortalContext portalContext) {
        this.portalContext = portalContext;
        initWidget(uiBinder.createAndBindUi(this));

        updateBundlesList();
        removeButton.addClickHandler(new BundleRemoveHandler());
        uploadButton.addClickHandler(new BundleUploadHandler());
        bundleList.addChangeHandler(new BundleListChangeHandler());
        HelpSystem.addClickHandler(showHelp, "managingBundles");
    }

    private void updateBundlesList() {
        processorsDesc = portalContext.getProcessors(BundleFilter.PROVIDER_USER);
        fillBundleList();
        updateBundleDetails();
    }

    private void fillBundleList() {
        if (processorsDesc.length > 0) {
            Set<String> bundles = new HashSet<String>(processorsDesc.length);
            for (DtoProcessorDescriptor descriptor : processorsDesc) {
                bundles.add(descriptor.getBundleName() + "-" + descriptor.getBundleVersion());
            }
            ArrayList<String> bundleNames = new ArrayList<String>(bundles);
            Collections.sort(bundleNames);
            for (String bundle : bundleNames) {
                bundleList.addItem(bundle);
            }
            bundleList.setSelectedIndex(0);
        }
    }

    private void updateBundleDetails() {
        processors.removeAllRows();
        final int selectedIndex = bundleList.getSelectedIndex();
        if (selectedIndex >= 0) {
            String bundleName = bundleList.getItemText(selectedIndex);
            int row = 0;
            FlexTable.FlexCellFormatter flexCellFormatter = processors.getFlexCellFormatter();
            for (DtoProcessorDescriptor descriptor : processorsDesc) {
                String name = descriptor.getBundleName() + "-" + descriptor.getBundleVersion();
                if (name.equals(bundleName)) {
                    flexCellFormatter.setStyleName(row, 0, style.explanatoryValue());
                    processors.setWidget(row, 0, new Label(descriptor.getProcessorName()));
                    processors.setWidget(row, 1, new Label(descriptor.getProcessorVersion()));
                    flexCellFormatter.setStyleName(row, 1, style.explanatoryValue());
                    row++;
                    processors.setWidget(row, 0, new HTML(descriptor.getDescriptionHtml()));
                    flexCellFormatter.setColSpan(row, 0, 2);
                    row++;
                    processors.setWidget(row, 0, new HTML("&nbsp;"));
                    flexCellFormatter.setColSpan(row, 0, 2);
                    row++;
                }
            }
        }
    }

    private class BundleRemoveHandler implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            final int selectedIndex = bundleList.getSelectedIndex();
            final String bundleName;
            if (selectedIndex >= 0) {
                bundleName = bundleList.getItemText(selectedIndex);
            } else {
                bundleName = null;
            }
            if (bundleName != null) {
                Dialog.ask("Remove Bundle",
                           new HTML("The bundle '" + bundleName + "' will be permanently deleted.<br/>" +
                                    "Do you really want to continue?"),
                           new Runnable() {
                               @Override
                               public void run() {
                                   removeBundle(bundleName);
                               }
                           });
            } else {
                Dialog.error("Remove Bundle",
                             "No bundle selected.");
            }

        }

        private void removeBundle(String bundleName) {
            portalContext.getBackendService().removeUserDirectory(BUNDLE_DIRECTORY + "/" + bundleName, new AsyncCallback<Boolean>() {
                @Override
                public void onFailure(Throwable caught) {
                    Dialog.error("Remove Bundle",
                                 "No bundle selected.");
                }

                @Override
                public void onSuccess(Boolean result) {
                    Dialog.info("Remove Bundle",
                                "Bundle has been successfully removed.");
                }
            });
        }
    }

    private class BundleUploadHandler implements ClickHandler {

        private final FileUpload bundleFileUpload;
        private final FormPanel uploadForm;
        private Dialog fileUploadDialog;
        private Dialog monitorDialog;
        private FormPanel.SubmitEvent submitEvent;

        private BundleUploadHandler() {
            bundleFileUpload = new FileUpload();
            bundleFileUpload.setName("bundleUpload");
            // this is more like a hint. It is up to the browser how it will consider the MIME type
            bundleFileUpload.getElement().setPropertyString("accept", "application/zip");
            uploadForm = new FormPanel();
            uploadForm.setWidget(bundleFileUpload);

            final BundleSubmitHandler submitHandler = new BundleSubmitHandler();
            FileUploadManager.configureForm(uploadForm,
                                            "dir=" + BUNDLE_DIRECTORY + "&bundle=true",
                                            submitHandler,
                                            submitHandler);
        }

        @Override
        public void onClick(ClickEvent event) {
            VerticalPanel verticalPanel = UIUtils.createVerticalPanel(2,
                                                                      new HTML("Select a bundle file:"),
                                                                      uploadForm,
                                                                      new HTML(
                                                                              "The bundle file must be <b>*.zip</b> file containing a bundle descriptor XML file.</br>" +
                                                                              "Uploads will replace existing bundles if they have the same name and version."));
            fileUploadDialog = new Dialog("File Upload", verticalPanel, Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
                @Override
                protected void onOk() {
                    String filename = bundleFileUpload.getFilename();
                    if (filename == null || filename.isEmpty()) {
                        Dialog.error("Bundle Upload",
                                     new HTML("No filename selected."),
                                     new HTML("Please specify a bundle file (*.zip)."));
                        return;
                    } else if (!filename.endsWith(".zip")) {
                        Dialog.error("Bundle Upload",
                                     new HTML("Not a valid bundle file selected."),
                                     new HTML("Please specify a bundle file (*.zip)."));
                    }
                    monitorDialog = new Dialog("Bundle Upload", new Label("Sending '" + filename + "'..."), ButtonType.CANCEL) {
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


        private class BundleSubmitHandler implements FormPanel.SubmitHandler, FormPanel.SubmitCompleteHandler {


            @Override
            public void onSubmit(FormPanel.SubmitEvent submitEvent) {
                BundleUploadHandler.this.submitEvent = submitEvent;
            }

            @Override
            public void onSubmitComplete(FormPanel.SubmitCompleteEvent submitCompleteEvent) {
                closeDialogs();
                ManageBundleForm.this.updateBundlesList();
                updateBundleDetails();
                String resultHTML = submitCompleteEvent.getResults();
                if (resultHTML.equals(bundleFileUpload.getFilename())) {
                    Dialog.info("Bundle Upload", "File successfully uploaded.");
                } else {
                    Dialog.error("Bundle Upload", "Error while bundle upload.<br/>" + resultHTML);
                }
            }
        }
    }

    private class BundleListChangeHandler implements ChangeHandler {

        @Override
        public void onChange(ChangeEvent event) {
            updateBundleDetails();
        }
    }
}
