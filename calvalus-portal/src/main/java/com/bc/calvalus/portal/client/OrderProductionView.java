package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.bc.calvalus.portal.shared.DtoProductionResponse;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;

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
        Window.alert("Failed to order production:\n" + failure.getMessage());
    }

    /**
     * Orders the production created by {@link #getProductionRequest()}.
     */
    protected void orderProduction() {
        if (validateForm()) {
            DtoProductionRequest request = getProductionRequest();
            getPortal().getBackendService().orderProduction(request, new AsyncCallback<DtoProductionResponse>() {
                public void onSuccess(final DtoProductionResponse response) {
                    onOrderProductionSuccess();
                }

                public void onFailure(Throwable caught) {
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
            ShowProductionRequestAction.run(getTitle(), getProductionRequest());
        }
    }
}