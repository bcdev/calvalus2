package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Document;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * A form that lets users select a product set.
 *
 * @author Norman
 */
public class ProductSetSelectionForm extends Composite {

    interface TheUiBinder extends UiBinder<Widget, ProductSetSelectionForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);
    private static final DtoProductSet[] EMPTY_PRODUCT_SETS = new DtoProductSet[0];

    private final PortalContext portal;
    private final Filter<DtoProductSet> productSetFilter;

    private DtoProductSet[] currentProductSets;
    private ProductSetSelectionForm.UpdateProductSetsCallback callback;

    @UiField
    CheckBox predefinedProductSets;
    @UiField
    CheckBox userProductionProductSets;
    @UiField
    CheckBox allProductionProductSets;

    @UiField
    ListBox productSetListBox;

    @UiField
    Label productSetName;
    @UiField
    Label productSetType;
    @UiField
    Label productSetStartDate;
    @UiField
    Label productSetEndDate;
    @UiField
    Label productSetRegionName;
    @UiField
    Label productSetGeoInventory;
    @UiField
    Anchor showProductSetSelectionHelp;

    public ProductSetSelectionForm(PortalContext portal) {
        this(portal, null);
    }

    public ProductSetSelectionForm(PortalContext portalContext, Filter<DtoProductSet> productSetFilter) {
        this.portal = portalContext;
        this.productSetFilter = productSetFilter;

        initWidget(uiBinder.createAndBindUi(this));

        callback = new UpdateProductSetsCallback();

        productSetListBox.addChangeHandler(event -> updateDetailsView());

        ValueChangeHandler<Boolean> valueChangeHandler = booleanValueChangeEvent -> updateProductSetsListBox();
        predefinedProductSets.addValueChangeHandler(valueChangeHandler);
        userProductionProductSets.addValueChangeHandler(valueChangeHandler);
        allProductionProductSets.addValueChangeHandler(valueChangeHandler);

        allProductionProductSets.setEnabled(portal.withPortalFeature("otherSets"));
        updateListBox(portal.getProductSets());
        updateDetailsView();

        HelpSystem.addClickHandler(showProductSetSelectionHelp, "productSetSelection");
    }

    private void updateProductSetsListBox() {
        String filter = getFilter();
        if (filter.isEmpty()) {
            if (predefinedProductSets.getValue()) {
                updateListBox(portal.getProductSets());
            } else {
                updateListBox(EMPTY_PRODUCT_SETS);
            }
        } else {
            portal.getBackendService().getProductSets(filter, callback);
        }
    }

    private String getFilter() {
        if (userProductionProductSets.getValue() && allProductionProductSets.getValue()) {
            return "user=all";
        } else if (userProductionProductSets.getValue()) {
            return "user=dummy";
        }
        return "";
    }

    private void updateListBox(DtoProductSet[] newProductSets) {
        DtoProductSet[] filteredProductSets = newProductSets;
        if (productSetFilter != null) {
            ArrayList<DtoProductSet> filtered = new ArrayList<>(newProductSets.length);
            for (DtoProductSet productSet : newProductSets) {
                if (productSetFilter.accept(productSet)) {
                    filtered.add(productSet);
                }
            }
            filteredProductSets = filtered.toArray(new DtoProductSet[filtered.size()]);
        }
        DtoProductSet oldSelection = null;
        if (currentProductSets != null) {
            int selectedIndex = productSetListBox.getSelectedIndex();
            if (selectedIndex != -1) {
                oldSelection = currentProductSets[selectedIndex];
            }
        }
        currentProductSets = filteredProductSets;
        productSetListBox.clear();
        int newSelectionIndex = 0;
        boolean productSetChanged = true;
        for (DtoProductSet productSet : currentProductSets) {
            productSetListBox.addItem(productSet.getName());
            if (oldSelection != null && oldSelection.equals(productSet)) {
                newSelectionIndex = productSetListBox.getItemCount() - 1;
                productSetChanged = false;
            }
        }
        if (productSetListBox.getItemCount() > 0) {
            productSetListBox.setSelectedIndex(newSelectionIndex);
        }
        if (productSetChanged) {
            DomEvent.fireNativeEvent(Document.get().createChangeEvent(), productSetListBox);
        }
    }

    private void updateDetailsView() {
        DtoProductSet productSet = getSelectedProductSet();
        if (productSet != null) {
            productSetName.setText(productSet.getName());
            productSetType.setText(productSet.getProductType());
            Date minDate = productSet.getMinDate();
            if (minDate != null) {
                productSetStartDate.setText(ProductSetFilterForm.DATE_FORMAT.format(minDate));
            } else {
                productSetStartDate.setText("");
            }
            Date maxDate = productSet.getMaxDate();
            if (maxDate != null) {
                productSetEndDate.setText(ProductSetFilterForm.DATE_FORMAT.format(maxDate));
            } else {
                productSetEndDate.setText("");
            }
            String regionName = productSet.getRegionName();
            productSetRegionName.setText(regionName != null ? regionName : "");
            String geoInventory = productSet.getGeoInventory();
            productSetGeoInventory.setText(geoInventory != null ? "Yes" : "No");
        } else {
            productSetName.setText("");
            productSetType.setText("");
            productSetStartDate.setText("");
            productSetEndDate.setText("");
            productSetRegionName.setText("");
            productSetGeoInventory.setText("");
        }
    }

    public void addChangeHandler(final ChangeHandler changeHandler) {
        productSetListBox.addChangeHandler(changeEvent -> changeHandler.onProductSetChanged(getSelectedProductSet()));
    }

    public DtoProductSet getSelectedProductSet() {
        int selectedIndex = productSetListBox.getSelectedIndex();
        if (selectedIndex >= 0) {
            return currentProductSets[selectedIndex];
        } else {
            return null;
        }
    }

    public void validateForm() throws ValidationException {
        DtoProductSet productSet = getSelectedProductSet();
        boolean productSetValid = productSet != null;
        if (!productSetValid) {
            throw new ValidationException(productSetListBox, "An input product set must be selected.");
        }
    }

    public interface ChangeHandler {
        void onProductSetChanged(DtoProductSet productSet);
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<>();
        DtoProductSet selectedProductSet = getSelectedProductSet();
        if (selectedProductSet.getGeoInventory() != null) {
            parameters.put("geoInventory", selectedProductSet.getGeoInventory());
        } else {
            parameters.put("inputPath", selectedProductSet.getPath());
        }
        return parameters;
    }

    AsyncCallback<DtoInputSelection> getInputSelectionCallback(){
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
            GWT.log("unable to access inputSelection", caught);
        }
    }

    private Map<String, String> parseParametersFromContext(DtoInputSelection inputSelection) {
        Map<String, String> parameters = new HashMap<>();
        if (inputSelection != null) {
            parameters.put("geoInventory", inputSelection.getCollectionName());
            parameters.put("collectionName", inputSelection.getCollectionName());
        }
        return parameters;
    }

    public void setValues(Map<String, String> parameters) {
        String collectionName = parameters.get("collectionName");
        String geoInventory = parameters.get("geoInventory");
        String inputPath = parameters.get("inputPath");
        int newSelectionIndex = -2;
        for (int i = 0; i < currentProductSets.length; i++) {
            DtoProductSet productSet = currentProductSets[i];
            if (collectionName != null && productSet.getName().equalsIgnoreCase(collectionName)) {
                newSelectionIndex = i;
                break;
            }
            if ((geoInventory != null &&
                    productSet.getGeoInventory() != null &&
                    geoInventory.equals(productSet.getGeoInventory()))
                    || (inputPath != null &&
                    productSet.getPath() != null &&
                    inputPath.equals(productSet.getPath()))) {
                newSelectionIndex = i;
                break;
            }
        }
        // TODO handle error

        if (newSelectionIndex != productSetListBox.getSelectedIndex()) {
            productSetListBox.setSelectedIndex(newSelectionIndex);
            DomEvent.fireNativeEvent(Document.get().createChangeEvent(), productSetListBox);
        }
    }


    private class UpdateProductSetsCallback implements AsyncCallback<DtoProductSet[]> {

        @Override
        public void onSuccess(DtoProductSet[] userProductSets) {
            if (predefinedProductSets.getValue()) {
                DtoProductSet[] predefined = portal.getProductSets();
                DtoProductSet[] combinedProductSets = new DtoProductSet[predefined.length + userProductSets.length];
                System.arraycopy(userProductSets, 0, combinedProductSets, 0, userProductSets.length);
                System.arraycopy(predefined, 0, combinedProductSets, userProductSets.length, predefined.length);
                updateListBox(combinedProductSets);
            } else {
                updateListBox(userProductSets);
            }
        }

        @Override
        public void onFailure(Throwable caught) {
            caught.printStackTrace(System.err);
            Dialog.error("Server-side Error", caught.getMessage());
            updateListBox(EMPTY_PRODUCT_SETS);
        }
    }
}