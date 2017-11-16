package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.InlineLabel;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * A form that lets users select a product.
 *
 * @author Hans
 */
public class ProductSelectionForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, ProductSelectionForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    private final PortalContext portal;

    @UiField
    ListBox productListBox;

    @UiField
    InlineLabel inputFileCount;

    @UiField
    Button pasteFromCatalogueButton;

    @UiField
    Button clearSelectionButton;

    ProductSelectionForm(PortalContext portalContext) {
        this.portal = portalContext;
        initWidget(uiBinder.createAndBindUi(this));
    }

    public void setValues(Map<String, String> parameters) {
        productListBox.clear();
        String productIdentifiersString = parameters.get("productIdentifiers");
        if (productIdentifiersString != null && !"".equals(productIdentifiersString)) {
            String[] productIdentifiers = productIdentifiersString.split(",");
            for (String product : productIdentifiers) {
                productListBox.addItem(product);
            }
            inputFileCount.setText(String.valueOf(productListBox.getItemCount()));
        }
    }

    public interface ClickHandler {

        void onClearSelectionClick();
    }

    void removeSelections() {
        Map<String, String> emptyInputSelectionMap = new HashMap<>();
        setValues(emptyInputSelectionMap);
    }

    void addInputSelectionChangeHandler(AsyncCallback<DtoInputSelection> callback) {
        pasteFromCatalogueButton.addClickHandler(event -> portal.getContextRetrievalService().getInputSelection(callback));
    }

    void addClearSelectionHandler(ClickHandler clickHandler) {
        clearSelectionButton.addClickHandler(event -> clickHandler.onClearSelectionClick());
    }

    AsyncCallback<DtoInputSelection> getInputSelectionCallback() {
        return new UpdateProductListCallback();
    }

    private class UpdateProductListCallback implements AsyncCallback<DtoInputSelection> {

        @Override
        public void onSuccess(DtoInputSelection inputSelection) {
            Map<String, String> inputSelectionMap = parseParametersFromContext(inputSelection);
            setValues(inputSelectionMap);
        }

        @Override
        public void onFailure(Throwable caught) {
            productListBox.clear();
        }
    }

    private Map<String, String> parseParametersFromContext(DtoInputSelection inputSelection) {
        Map<String, String> parameters = new HashMap<>();
        if (inputSelection != null) {
            if (inputSelection.getProductIdentifiers() != null) {
                parameters.put("productIdentifiers", String.join(",", inputSelection.getProductIdentifiers()));
            } else {
                parameters.put("productIdentifiers", "");
            }
        }
        return parameters;
    }
}