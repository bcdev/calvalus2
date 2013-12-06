package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marco Peters
 */
public class BootstrappingForm extends Composite {

    private static final int DEFAULT_NUMBER_OF_ITERATIONS = 10000;
    private final PortalContext portalContext;
    private final UserManagedFiles userManagedContent;


    interface TheUiBinder extends UiBinder<Widget, BootstrappingForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox boostrapSources;
    @UiField
    Button addBoostrapSourceButton;
    @UiField
    Button removeBoostrapSourceButton;

    @UiField
    ListBox processorList;
    @UiField
    IntegerBox numberOfIterations;
    @UiField
    TextBox productionName;

    public BootstrappingForm(PortalContext portalContext) {
        this.portalContext = portalContext;
        initWidget(uiBinder.createAndBindUi(this));

        numberOfIterations.setValue(DEFAULT_NUMBER_OF_ITERATIONS);

        HTML description = new HTML("The supported file types are TAB-separated CSV (<b>*.csv</b>) matchup files.<br/>");
        userManagedContent = new UserManagedFiles(portalContext.getBackendService(),
                                                  boostrapSources,
                                                  "boostrapping",
                                                  "matchup",
                                                  description);
        addBoostrapSourceButton.addClickHandler(userManagedContent.getAddAction());
        removeBoostrapSourceButton.addClickHandler(userManagedContent.getRemoveAction());
    }

    public void validateForm() throws ValidationException {

        Integer numberOfIterationsValue = numberOfIterations.getValue();
        if (numberOfIterationsValue == null || numberOfIterationsValue <= 0) {
            throw new ValidationException(numberOfIterations, "Number of Iterations must be > 0");
        }

        boolean boostrapSourceValid = boostrapSources.getSelectedIndex() >= 0;
        if (!boostrapSourceValid) {
            throw new ValidationException(boostrapSources, "Boostrap source must be given.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("calvalus.bootstrap.numberOfIterations", numberOfIterations.getText());
        parameters.put("calvalus.bootstrap.inputFile", userManagedContent.getSelectedFilename());
        parameters.put("productionName", productionName.getValue());
        return parameters;
    }

}
