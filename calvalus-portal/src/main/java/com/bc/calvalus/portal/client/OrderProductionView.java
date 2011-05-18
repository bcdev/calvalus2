package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProductionRequest;
import com.bc.calvalus.portal.shared.GsProductionResponse;
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
    protected abstract GsProductionRequest getProductionRequest();

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
            GsProductionRequest request = getProductionRequest();
            getPortal().getBackendService().orderProduction(request, new AsyncCallback<GsProductionResponse>() {
                public void onSuccess(final GsProductionResponse response) {
                    onOrderProductionSuccess();
                }

                public void onFailure(Throwable caught) {
                    onOrderProductionFailure(caught);
                }
            });
        }
    }
}