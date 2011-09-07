package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.google.gwt.cell.client.EditTextCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.SelectionCell;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Style;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.maps.client.geom.LatLng;
import com.google.gwt.maps.client.geom.LatLngBounds;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.Math.*;

/**
 * Form for MA (matup analysis) parameters.
 *
 * @author Norman
 */
public class MAParametersForm extends Composite {

    private final Map<String, DtoProcessorVariable> processorVariableDefaults;

    interface TheUiBinder extends UiBinder<Widget, MAParametersForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
     ListBox recordSources;
     @UiField
     Button addRecordSourceButton;
     @UiField
     Button removeRecordSourceButton;

    @UiField
    IntegerBox macroPixelSize;
    @UiField
    DoubleBox maxTimeDifference;
    @UiField
    DoubleBox filteredMeanCoeff;
    @UiField
    TextBox outputGroupName;

    @UiField
    TextBox goodPixelExpression;
    @UiField
    TextBox goodRecordExpression;


    public MAParametersForm() {
        processorVariableDefaults = new HashMap<String, DtoProcessorVariable>();

        initWidget(uiBinder.createAndBindUi(this));

        addRecordSourceButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                // todo - show upload dialog
            }
        });

        removeRecordSourceButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                // todo - show upload dialog
            }
        });

        macroPixelSize.setValue(5);
        maxTimeDifference.setValue(3.0);
        filteredMeanCoeff.setValue(1.5);
        outputGroupName.setValue("SITE");

        recordSources.addItem("file1.txt");
        recordSources.addItem("file2.csv");
        recordSources.addItem("file3.placemark");
        recordSources.setSelectedIndex(0);

    }

    public void setSelectedProcessor(DtoProcessorDescriptor selectedProcessor) {
        if (selectedProcessor == null) {
            return;
        }
        processorVariableDefaults.clear();
        goodPixelExpression.setValue(selectedProcessor.getDefaultMaskExpr());
        goodRecordExpression.setValue(selectedProcessor.getProcessorVariables()[0].getName() + ".cv < 0.15");
    }

    public void validateForm() throws ValidationException {
        boolean macroPixelSizeValid = macroPixelSize.getValue() >= 1
                && macroPixelSize.getValue() <= 31
                && macroPixelSize.getValue() % 2 == 1;
        if (!macroPixelSizeValid) {
            throw new ValidationException(macroPixelSize, "Macro pixel size must be an odd integer between 1 and 31");
        }
        boolean filteredMeanCoeffValid = filteredMeanCoeff.getValue() > 0;
        if (!filteredMeanCoeffValid) {
            throw new ValidationException(filteredMeanCoeff, "Filtered mean coefficient must be >= 1");
        }

        boolean maxTimeDifferenceValid = maxTimeDifference.getValue() >= 0;
         if (!maxTimeDifferenceValid) {
             throw new ValidationException(maxTimeDifference, "Max. time difference must be >= 0 hours (0 disables time criterion)");
         }

        boolean outputGroupNameValid = !outputGroupName.getText().trim().isEmpty();
              if (!outputGroupNameValid) {
                  throw new ValidationException(maxTimeDifference, "Output group name must be given.");
              }

        boolean recordSourceValid = recordSources.getSelectedIndex() >= 0;
          if (!recordSourceValid) {
                  throw new ValidationException(maxTimeDifference, "In-situ record source must be given.");
              }

    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("copyInput", "true");
        parameters.put("goodPixelExpression", goodPixelExpression.getText());
        parameters.put("goodRecordExpression", goodRecordExpression.getText());
        parameters.put("macroPixelSize", macroPixelSize.getText());
        parameters.put("maxTimeDifference", maxTimeDifference.getText());
        parameters.put("filteredMeanCoeff", filteredMeanCoeff.getText());
        parameters.put("outputGroupName", outputGroupName.getText());
        int selectedIndex = recordSources.getSelectedIndex();
        parameters.put("recordSourceUrl", selectedIndex >= 0 ? recordSources.getValue(selectedIndex) : "");
        return parameters;
    }

 }