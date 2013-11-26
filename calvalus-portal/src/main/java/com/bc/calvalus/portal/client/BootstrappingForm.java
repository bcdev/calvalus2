package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

/**
 * @author Marco Peters
 */
public class BootstrappingForm extends Composite {

    private static final int DEFAULT_NUMBER_OF_ITERATIONS = 10000;
    private final PortalContext portalContext;

    interface TheUiBinder extends UiBinder<Widget, BootstrappingForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    FileUpload csvFileUpload;
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
        csvFileUpload.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                // todo - upload the chosen file
            }
        });
    }
}
