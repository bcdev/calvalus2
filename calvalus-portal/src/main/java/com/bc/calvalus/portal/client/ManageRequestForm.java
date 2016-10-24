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
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.Date;

/**
 * Form for handling production requests
 */
public class ManageRequestForm extends Composite {

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
    FlexTable requestParameterTable;

    private DtoProductionRequest[] productionRequests;

    public ManageRequestForm(PortalContext portalContext) {
        this.portalContext = portalContext;
        initWidget(uiBinder.createAndBindUi(this));

        updateRequestList();
        removeButton.addClickHandler(new RequestRemoveHandler());
        editButton.addClickHandler(new RequestEditHandler());
        requestList.addChangeHandler(new RequestListChangeHandler());
//        HelpSystem.addClickHandler(showHelp, "managingRequest");
    }


    void updateRequestList() {
        GWT.log("updateRequestList start " + new Date());
        portalContext.getBackendService().listRequests(new AsyncCallback<DtoProductionRequest[]>() {
            @Override
            public void onSuccess(DtoProductionRequest[] result) {
                productionRequests = result;
                fillRequestList();
                updateRequestDetails();
                GWT.log("updateRequestList end " + new Date());
            }

            @Override
            public void onFailure(Throwable caught) {
                caught.printStackTrace(System.err);
                Dialog.error("Server-side Error", caught.getMessage());
                productionRequests = new DtoProductionRequest[0];
                fillRequestList();
                updateRequestDetails();
            }
        });
    }

    private void fillRequestList() {
        requestList.clear();
        if (productionRequests.length > 0) {
            for (DtoProductionRequest productionRequest : productionRequests) {
                requestList.addItem(formatListEntry(productionRequest));
            }
            requestList.setSelectedIndex(0);
        }
    }

    private static String formatListEntry(DtoProductionRequest productionRequest) {
        String id = productionRequest.getId();
        String entry = "";
        if (id.contains("_")) {
            String[] split = id.split("_");
            if (split.length == 3) {
                if (split[0].length() == 8) {
                    String year = split[0].substring(0, 4);
                    String month = split[0].substring(4, 6);
                    String day = split[0].substring(6);
                    entry += day + "." + month + "." + year;
                } else {
                    return id;
                }
                if (split[1].length() == 9) {
                    String hour = split[1].substring(0, 2);
                    String minute = split[1].substring(2, 4);
                    String sec = split[1].substring(4, 6);
                    entry += " " + hour + ":" + minute + ":" + sec;
                } else {
                    return id;
                }
                entry += " " + split[2];
                String productionName = productionRequest.getProductionParameters().get("productionName");
                if (productionName != null) {
                    entry += " " + productionName;
                }
                return entry;
            }
        }
        return id;
    }

    private void updateRequestDetails() {
        requestParameterTable.removeAllRows();
        final int selectedIndex = requestList.getSelectedIndex();
        if (selectedIndex >= 0) {
            ShowProductionRequestAction.fillTable(requestParameterTable, productionRequests[selectedIndex]);
            removeButton.setEnabled(true);
            editButton.setEnabled(true);
        } else {
            removeButton.setEnabled(false);
            editButton.setEnabled(false);
        }
    }

    private class RequestEditHandler implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            final int selectedIndex = requestList.getSelectedIndex();
            if (selectedIndex >= 0) {
                DtoProductionRequest request = productionRequests[selectedIndex];
                String productionType = request.getProductionType();
                OrderProductionView orderProductionView = portalContext.getViewForRestore(productionType);
                orderProductionView.setProductionParameters(request.getProductionParameters());
                portalContext.showView(orderProductionView.getViewId());
            }
        }
    }

    private class RequestRemoveHandler implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            final int selectedIndex = requestList.getSelectedIndex();
            final String requestName;
            if (selectedIndex >= 0) {
                requestName = requestList.getItemText(selectedIndex);
            } else {
                requestName = null;
            }
            if (requestName != null) {
                Dialog.ask("Remove Request",
                           new HTML("The request '" + requestName + "' will be permanently deleted.<br/>" +
                                            "Do you really want to continue?"),
                           new Runnable() {
                               @Override
                               public void run() {
                                   removeRequest(productionRequests[selectedIndex].getId());
                               }
                           });
            } else {
                Dialog.error("Remove Request",
                             "No request selected.");
            }

        }

        private void removeRequest(String requestId) {
            portalContext.getBackendService().deleteRequest(requestId, new AsyncCallback<Void>() {
                @Override
                public void onFailure(Throwable caught) {
                    Dialog.error("Remove Request",
                                 "Error while removing request.");
                }

                @Override
                public void onSuccess(Void result) {
                    Dialog.info("Remove Request",
                                "Request has been successfully removed.");
                    updateRequestList();
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
