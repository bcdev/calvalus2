package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
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
public class ProductsFromCatalogueForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, ProductsFromCatalogueForm> {

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
    InlineLabel warningMessage;

    @UiField
    Button pasteFromCatalogueButton;

    @UiField
    Button clearSelectionButton;

    ProductsFromCatalogueForm(PortalContext portalContext) {
        this.portal = portalContext;
        initWidget(uiBinder.createAndBindUi(this));

        productListTextArea.addChangeHandler(event -> {
            parseProductList();
            updateProductListTextArea();
            updateFileCount();
        });
    }

    public void validateForm(String collectionNameSelected) throws ValidationException {
        if (productIdentifiersCount > 0 &&
            collectionName != null &&
            !collectionNameSelected.equals(collectionName)) {
            throw new ValidationException(this,
                                          "The selected input files are not consistent with the selected input file set. " +
                                          "To change the input file set, please first clear the input files selection");
        }
        if (warningMessage.getText().endsWith("Nothing to process.")) {
            throw new ValidationException(this, "The selected inputs from catalogue are not contained in any processing" +
                    " collection. Please select a different set of inputs.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<>();
        String[] productIdentifierList = productListTextArea.getText().split("\n");
        parameters.put("productIdentifiers", String.join(",", productIdentifierList));
        return parameters;
    }

    public void setValues(Map<String, String> parameters) {
        productIdentifiersString = parameters.get("productIdentifiers");
        if (productIdentifiersString != null && productIdentifiersString.length() > 0) {
            collectionName = parameters.get("collectionName");
        } else {
            collectionName = null;
        }
        updateProductListTextArea();
        updateFileCount();
        updateWarningMessage(parameters.get("warningMessage"));
    }

    // do not remove the static modifier even though suggested by IntelliJ
    // https://stackoverflow.com/a/21930980/2676893
    public static interface InputSelectionHandler {

        AsyncCallback<DtoInputSelection> getInputSelectionChangedCallback();

        void onClearSelectionClick();
    }

    void removeSelections() {
        Map<String, String> emptyInputSelectionMap = new HashMap<>();
        setValues(emptyInputSelectionMap);
    }

    void addInputSelectionHandler(InputSelectionHandler inputSelectionHandler) {
        pasteFromCatalogueButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                portal.getBackendService().getInputSelection(portal.getUserName(), inputSelectionHandler.getInputSelectionChangedCallback());

            }
        });
        clearSelectionButton.addClickHandler(event -> inputSelectionHandler.onClearSelectionClick());
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

    private void updateWarningMessage(String warningMessage) {
        this.warningMessage.setText(warningMessage != null ? warningMessage : "");
    }

}