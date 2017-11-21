package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.InlineLabel;
import com.google.gwt.user.client.ui.TextArea;
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

    private String productIdentifiersString;
    private String collectionName;
    private int productIdentifiersCount;

    @UiField
    TextArea productListTextArea;

    @UiField
    InlineLabel inputFileCount;

    @UiField
    Button pasteFromCatalogueButton;

    @UiField
    Button clearSelectionButton;

    ProductSelectionForm(PortalContext portalContext) {
        this.portal = portalContext;
        initWidget(uiBinder.createAndBindUi(this));

        productListTextArea.addChangeHandler(event -> {
            parseProductList();
            updateProductListTextArea();
            updateFileCount();
        });
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<>();
        String[] productIdentifierList = productListTextArea.getText().split("\n");
        parameters.put("collectionName", collectionName);
        parameters.put("productIdentifiers", String.join(",", productIdentifierList));
        return parameters;
    }

    public void setValues(Map<String, String> parameters) {
        productIdentifiersString = parameters.get("productIdentifiers");
        collectionName = parameters.get("collectionName");
        updateProductListTextArea();
        updateFileCount();
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

    private void parseProductList() {
        String[] productIdentifierList = productListTextArea.getText().split("\n");
        productIdentifiersString = String.join(",", productIdentifierList);
    }

    private void updateProductListTextArea() {
        String productIdentifiersText = "";
        productIdentifiersCount = 0;
        if (productIdentifiersString != null && !"".equals(productIdentifiersString)) {
            String[] productIdentifiersArray = productIdentifiersString.split(",");
            productIdentifiersCount = productIdentifiersArray.length;
            productIdentifiersText = String.join("\n", productIdentifiersArray);
        }
        productListTextArea.setText(productIdentifiersText);
    }

    private void updateFileCount() {
        inputFileCount.setText(String.valueOf(productIdentifiersCount));
    }
}