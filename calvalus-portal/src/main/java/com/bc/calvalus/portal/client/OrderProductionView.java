package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.bc.calvalus.portal.shared.DtoProductionResponse;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;

import java.util.Map;

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
     * Called by {@link #orderProduction} and {@link #checkRequest()} and {@link #saveRequest()}.
     *
     * @return The production request.
     */
    protected DtoProductionRequest getProductionRequest() {
        return new DtoProductionRequest(getProductionType(), getProductionParameters());
    }

    /**
     * Called by {@link #getProductionRequest()}.
     *
     * @return The production type.
     */
    protected abstract String getProductionType();

    /**
     * Called by {@link #getProductionRequest()}.
     *
     * @return The parameter map.
     */
    protected abstract Map<String, String> getProductionParameters();

    /**
     *
     * @return true, if the parameters of this view can be restored
     */
    public boolean isRestoringRequestPossible() {
        return false;
    }

    /**
     * Sets the view to the given parameters.
     *
     * The default implementation does nothing.
     *
     * @param parameters A map with the parameters.
     */
    public void setProductionParameters(Map<String, String> parameters) {
    }

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

            final DialogBox submitDialog = createSubmitProductionDialog();
            submitDialog.center();

            getPortal().getBackendService().orderProduction(request, new AsyncCallback<DtoProductionResponse>() {
                public void onSuccess(final DtoProductionResponse response) {
                    submitDialog.hide();
                    onOrderProductionSuccess();
                }

                public void onFailure(Throwable caught) {
                    submitDialog.hide();
                    onOrderProductionFailure(caught);
                }
            });
        }
    }

    static DialogBox createSubmitProductionDialog() {
        final DialogBox submitDialog = new DialogBox();
        submitDialog.ensureDebugId("cwDialogBox");
        submitDialog.setText("Submitting Production Request...");
        submitDialog.setGlassEnabled(true);
        submitDialog.setAnimationEnabled(true);
        submitDialog.setAutoHideEnabled(true);
        submitDialog.setWidget(new HTML("Your production request is currently being processed on the server. After a preparation phase<br/>" +
                                       "you can observe its processing progress in the <b>Manage Productions</b> tab.<br/>" +
                                       "Depending on the number of files in the input file set, the preparation phase may take<br/>" +
                                       "seconds to minutes.<br/>" +
                                       "<br/>" +
                                       "This dialog box will then close automatically."));
        return submitDialog;
    }

    /**
     * Validates and displays the request created by {@link #getProductionRequest()}.
     */
    protected void checkRequest() {
        if (validateForm()) {
            ShowProductionRequestAction.run("Valid " + getTitle() + " Request", getProductionRequest());
        }
    }

    protected void saveRequest() {
        DtoProductionRequest request = getProductionRequest();
        AsyncCallback<Void> callback = new AsyncCallback<Void>() {

            @Override
            public void onSuccess(Void result) {
                Dialog.info("Save Request", "Request successfully saved.");
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.info("Save Request", "Failed to safe request:\n" + caught.getMessage());
            }
        };
        getPortal().getBackendService().saveRequest(request, callback);
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

        Button saveButton = new Button("Save Request");
        saveButton.addClickHandler(new ClickHandler() {
            public void onClick(ClickEvent event) {
                saveRequest();
            }
        });

        HorizontalPanel buttonPanel = new HorizontalPanel();
        buttonPanel.setSpacing(4);
        buttonPanel.add(checkButton);
        buttonPanel.add(saveButton);
        buttonPanel.add(orderButton);

        HorizontalPanel orderPanel = new HorizontalPanel();
        orderPanel.setSpacing(4);
        orderPanel.setWidth("100%");
        orderPanel.add(buttonPanel);
        orderPanel.setCellHorizontalAlignment(buttonPanel, HasHorizontalAlignment.ALIGN_CENTER);
        return orderPanel;
    }
}