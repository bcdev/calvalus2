package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.bc.calvalus.portal.shared.DtoProductionResponse;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;

// todo: prefer composition over inheritance: make subclasses a component of OrderProductionView (which is then concrete) (nf)

/**
 * Base class for production views.
 *
 * @author Norman
 */
public abstract class OrderProductionView extends PortalView {

    protected OrderProductionView(PortalContext portalContext) {
        super(portalContext);
    }

    /**
     * Called by {@link #orderProduction}.
     *
     * @return The production request.
     */
    protected abstract DtoProductionRequest getProductionRequest();

    /**
     * Called by {@link #orderProduction}.
     *
     * @return true, if the form is valid.
     */
    protected abstract boolean validateForm();

    /**
     * Called by {@link #orderProduction}.
     * Default impl. opens the "Production Manager" page.
     */
    protected void onOrderProductionSuccess() {
        getPortal().showView(ManageProductionsView.ID);
    }

    /**
     * Called by {@link #orderProduction}.
     * Default impl. displays error message.
     *
     * @param failure The failure.
     */
    protected void onOrderProductionFailure(Throwable failure) {
        failure.printStackTrace(System.err);
        Dialog.error("Server-side Production Error", failure.getMessage());
    }

    /**
     * Orders the production created by {@link #getProductionRequest()}.
     */
    protected void orderProduction() {
        if (validateForm()) {
            DtoProductionRequest request = getProductionRequest();

            final DialogBox progressBox = new DialogBox();
            progressBox.ensureDebugId("cwDialogBox");
            progressBox.setText("Submitting Production Request...");
            progressBox.setGlassEnabled(true);
            progressBox.setAnimationEnabled(true);
            progressBox.setAutoHideEnabled(true);
            progressBox.setWidget(new HTML("Your production request is currently being processed on the server. After a preparation phase<br/>" +
                                                   "you can observe its processing progress in the <b>Manage Productions</b> tab.<br/>" +
                                                   "Depending on the number of files in the input file set, the preparation phase may take<br/>" +
                                                   "seconds to minutes.<br/>" +
                                                   "<br/>" +
                                                   "This dialog box will then close automatically."));
            progressBox.center();

            getPortal().getBackendService().orderProduction(request, new AsyncCallback<DtoProductionResponse>() {
                public void onSuccess(final DtoProductionResponse response) {
                    progressBox.hide();
                    onOrderProductionSuccess();
                }

                public void onFailure(Throwable caught) {
                    progressBox.hide();
                    onOrderProductionFailure(caught);
                }
            });
        }
    }

    /**
     * Validates and displays the request created by {@link #getProductionRequest()}.
     */
    protected void checkRequest() {
        if (validateForm()) {
            ShowProductionRequestAction.run("Valid " + getTitle() + " Request", getProductionRequest());
        }
    }

    protected HorizontalPanel createOrderPanel() {
        Button orderButton = new Button("Order Production");
        orderButton.addClickHandler(new ClickHandler() {
            public void onClick(ClickEvent event) {
                orderProduction();
            }
        });

        Button checkButton = new Button("Check Request");
        checkButton.addClickHandler(new ClickHandler() {
            public void onClick(ClickEvent event) {
                checkRequest();
            }
        });

        HorizontalPanel buttonPanel = new HorizontalPanel();
        buttonPanel.setSpacing(4);
        buttonPanel.add(checkButton);
        buttonPanel.add(orderButton);

        HorizontalPanel orderPanel = new HorizontalPanel();
        orderPanel.setSpacing(4);
        orderPanel.setWidth("100%");
        orderPanel.add(buttonPanel);
        orderPanel.setCellHorizontalAlignment(buttonPanel, HasHorizontalAlignment.ALIGN_CENTER);
        return orderPanel;
    }
}