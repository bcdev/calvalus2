/*
 * Copyright (C) 2022 Brockmann Consult GmbH (info@brockmann-consult.de)
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
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This form show setting related to production in general and output.
 *
 * @author Norman
 * @author MarcoZ
 */
public class TemplateSelectionForm extends Composite {

    private final PortalContext portalContext;

    interface TheUiBinder extends UiBinder<Widget, TemplateSelectionForm> {}

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);
    @UiField
    ListBox requestList;
    @UiField
    Button editButton;

    private DtoProductionRequest[] productionRequests;

    public TemplateSelectionForm(PortalContext portalContext) {
        this.portalContext = portalContext;
        initWidget(uiBinder.createAndBindUi(this));
        updateRequestList();
        editButton.addClickHandler(new TemplateSelectionForm.RequestEditHandler());
    }

    void updateRequestList() {
        GWT.log("updateRequestList start " + new Date());
        portalContext.getBackendService().listRequests(new AsyncCallback<DtoProductionRequest[]>() {
            @Override
            public void onSuccess(DtoProductionRequest[] result) {
                List<DtoProductionRequest> accu = new ArrayList<>();
                for (DtoProductionRequest r : result) {
                    if ("L3".equals(r.getProductionType())) {
                        accu.add(r);
                    }
                }
                productionRequests = accu.toArray(new DtoProductionRequest[accu.size()]);
                fillRequestList();
                GWT.log("updateRequestList end " + new Date());
            }

            @Override
            public void onFailure(Throwable caught) {
                caught.printStackTrace(System.err);
                Dialog.error("Server-side Error", caught.getMessage());
                productionRequests = new DtoProductionRequest[0];
                fillRequestList();
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

}