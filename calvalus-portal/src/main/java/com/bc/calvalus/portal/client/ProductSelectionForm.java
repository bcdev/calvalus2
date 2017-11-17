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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    private String productIdentifiersString;
    private String collectionName;

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

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<>();
        productListBox.getItemCount();
        List<String> productIdentifierList = new ArrayList<>();
        for (int i = 0; i < productListBox.getItemCount(); i++) {
            productIdentifierList.add(productListBox.getItemText(i));
        }
        parameters.put("collectionName", collectionName);
        parameters.put("productIdentifiers", String.join(",", productIdentifierList));
        return parameters;
    }

    public void setValues(Map<String, String> parameters) {
        productIdentifiersString = parameters.get("productIdentifiers");
        collectionName = parameters.get("collectionName");
        updateProductListBox();
    }

    // do not remove the static modifier even though suggested by IntelliJ
    // https://stackoverflow.com/a/21930980/2676893
    public static interface ClickHandler {

        AsyncCallback<DtoInputSelection> getInputSelectionChangedCallback();

        void onClearSelectionClick();
    }

    void removeSelections() {
        Map<String, String> emptyInputSelectionMap = new HashMap<>();
        setValues(emptyInputSelectionMap);
    }

    void addInputSelectionHandler(ClickHandler clickHandler) {
        pasteFromCatalogueButton.addClickHandler(event -> portal.getContextRetrievalService().
                    getInputSelection(clickHandler.getInputSelectionChangedCallback()));
        clearSelectionButton.addClickHandler(event -> clickHandler.onClearSelectionClick());
    }

    private void updateProductListBox() {
        productListBox.clear();
        if (productIdentifiersString != null && !"".equals(productIdentifiersString)) {
            String[] productIdentifiers = productIdentifiersString.split(",");
            for (String product : productIdentifiers) {
                productListBox.addItem(product);
            }
            inputFileCount.setText(String.valueOf(productListBox.getItemCount()));
        }
    }
}