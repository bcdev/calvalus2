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
import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
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
 * Form for handling production requests
 */
public class ManageRequestForm extends Composite {

    private static final String BUNDLE_DIRECTORY = "software";

    private final PortalContext portalContext;

    interface TheUiBinder extends UiBinder<Widget, ManageRequestForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    CalvalusStyle style;

    @UiField
    ListBox requestList;
    @UiField
    Button removeButton;
    @UiField
    Button editButton;
//    @UiField
//    Anchor showHelp;

    @UiField
    FlexTable request;

    private DtoProductionRequest[] productionRequests;

    public ManageRequestForm(PortalContext portalContext) {
        this.portalContext = portalContext;
        initWidget(uiBinder.createAndBindUi(this));

        removeButton.addClickHandler(new RequestRemoveHandler());
//        editButton.addClickHandler(new BundleUploadHandler());
        requestList.addChangeHandler(new RequestListChangeHandler());
//        HelpSystem.addClickHandler(showHelp, "managingRequest");
    }



    void updateRequestList() {
        portalContext.getBackendService().listRequests(new AsyncCallback<DtoProductionRequest[]>() {
            @Override
            public void onFailure(Throwable caught) {
                // TODO
            }

            @Override
            public void onSuccess(DtoProductionRequest[] result) {
                productionRequests = result;
                fillRequestList();
                updateRequestDetails();
            }
        });
    }

    private void fillRequestList    () {
        requestList.clear();
        if (productionRequests.length > 0) {
            for (DtoProductionRequest productionRequest : productionRequests) {
                requestList.addItem(productionRequest.getId());
            }
            requestList.setSelectedIndex(0);
        }
    }

    private void updateRequestDetails() {
//        processors.removeAllRows();
//        final int selectedIndex = requestList.getSelectedIndex();
//        if (selectedIndex >= 0) {
//            String bundleName = requestList.getItemText(selectedIndex);
//            int row = 0;
//            FlexTable.FlexCellFormatter flexCellFormatter = processors.getFlexCellFormatter();
//            for (DtoProcessorDescriptor descriptor : processorsDesc) {
//                String name = descriptor.getBundleName() + "-" + descriptor.getBundleVersion();
//                if (name.equals(bundleName)) {
//                    flexCellFormatter.setStyleName(row, 0, style.explanatoryValue());
//                    processors.setWidget(row, 0, new Label(descriptor.getProcessorName()));
//                    processors.setWidget(row, 1, new Label(descriptor.getProcessorVersion()));
//                    flexCellFormatter.setStyleName(row, 1, style.explanatoryValue());
//                    row++;
//                    processors.setWidget(row, 0, new HTML(descriptor.getDescriptionHtml()));
//                    flexCellFormatter.setColSpan(row, 0, 2);
//                    row++;
//                    processors.setWidget(row, 0, new HTML("&nbsp;"));
//                    flexCellFormatter.setColSpan(row, 0, 2);
//                    row++;
//                }
//            }
//        }
    }

    private class RequestRemoveHandler implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            final int selectedIndex = requestList.getSelectedIndex();
            final String bundleName;
            if (selectedIndex >= 0) {
                bundleName = requestList.getItemText(selectedIndex);
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
                                   updateRequestList();
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

    private class RequestListChangeHandler implements ChangeHandler {

        @Override
        public void onChange(ChangeEvent event) {
            updateRequestDetails();
        }
    }
}
