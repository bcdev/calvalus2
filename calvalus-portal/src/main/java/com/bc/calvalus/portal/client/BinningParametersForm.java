package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.HasValue;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class BinningParametersForm extends Composite {
    interface TheUiBinder extends UiBinder<Widget, BinningParametersForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox inputVariables;
    @UiField
    TextBox maskExpr;
    @UiField
    DoubleBox fillValue;
    @UiField
    ListBox aggregator;
    @UiField
    DoubleBox weightCoeff;

    @UiField
    IntegerBox steppingPeriodLength;
    @UiField
    IntegerBox compositingPeriodLength;
    @UiField
    IntegerBox periodCount;

    @UiField
    DoubleBox resolution;
    @UiField
    IntegerBox superSampling;

    @UiField
    IntegerBox targetWidth;
    @UiField
    IntegerBox targetHeight;

    HasValue<Date> minDate;
    HasValue<Date> maxDate;

    public BinningParametersForm() {
        initWidget(uiBinder.createAndBindUi(this));

        inputVariables.addItem("chl_conc");
        inputVariables.addItem("tsm");
        inputVariables.addItem("Z90_max");
        inputVariables.addItem("chiSquare");
        inputVariables.addItem("turbidity_index");
        inputVariables.setSelectedIndex(0);

        maskExpr.setText("!l1_flags.INVALID AND !l1_flags.LAND_OCEAN");

        fillValue.setValue(Double.NaN);

        aggregator.addItem("Average", "AVG");
        aggregator.addItem("Max. Likelihood Average", "AVG_ML");
        aggregator.addItem("Minimum + Maximum", "MIN_MAX");
        aggregator.addItem("P90 Percentile", "PERCENTILE");
        aggregator.setSelectedIndex(1);

        weightCoeff.setValue(0.5);
        superSampling.setValue(1);

        steppingPeriodLength.setValue(10);
        steppingPeriodLength.addValueChangeHandler(new ValueChangeHandler<Integer>() {
            @Override
            public void onValueChange(ValueChangeEvent<Integer> event) {
                updatePeriodCount();
            }
        });

        compositingPeriodLength.setValue(10);

        periodCount.setValue(0);
        periodCount.setEnabled(false);

        resolution.setValue(9.28);

        targetWidth.setValue(0);
        targetWidth.setEnabled(false);

        targetHeight.setValue(0);
        targetHeight.setEnabled(false);
    }

    public void setTimeRange(HasValue<Date> minDate, HasValue<Date> maxDate) {
        this.minDate = minDate;
        this.maxDate = maxDate;
        updatePeriodCount();
    }

    private void updatePeriodCount() {
        long millisPerDay = 24L * 60L * 60L * 1000L;
        long deltaMillis = maxDate.getValue().getTime() - minDate.getValue().getTime();
        int deltaDays = (int) ((millisPerDay + deltaMillis) / millisPerDay);
        periodCount.setValue(deltaDays / steppingPeriodLength.getValue());
    }

    public void validateForm() throws ValidationException {
        boolean periodCountValid = periodCount.getValue() >= 1;
        if (!periodCountValid) {
            throw new ValidationException(periodCount, "Period count must be >= 1");
        }

        boolean periodLengthValid = steppingPeriodLength.getValue() >= 1;
        if (!periodLengthValid) {
            throw new ValidationException(steppingPeriodLength, "Period length must be >= 1");
        }

        boolean compositingPeriodLengthValid = compositingPeriodLength.getValue() >= 1 && compositingPeriodLength.getValue() <= steppingPeriodLength.getValue();
        if (!compositingPeriodLengthValid) {
            throw new ValidationException(compositingPeriodLength, "Compositing period length must be >= 1 and less or equal to than period");
        }

        boolean weightCoeffValid = weightCoeff.getValue() >= 0.0 && weightCoeff.getValue() <= 1.0;
        if (!weightCoeffValid) {
            throw new ValidationException(weightCoeff, "Weight coefficient must be >= 0 and <= 1");
        }

        boolean resolutionValid = resolution.getValue() > 0.0;
        if (!resolutionValid) {
            throw new ValidationException(resolution, "Resolution must greater than zero");
        }

        boolean superSamplingValid = superSampling.getValue() >= 1 && superSampling.getValue() <= 9;
        if (!superSamplingValid) {
            throw new ValidationException(superSampling, "Super-sampling must be >= 1 and <= 9");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("inputVariables", inputVariables.getValue(inputVariables.getSelectedIndex()));
        parameters.put("maskExpr", maskExpr.getText());
        parameters.put("fillValue", fillValue.getText());
        parameters.put("aggregator", aggregator.getValue(aggregator.getSelectedIndex()));
        parameters.put("weightCoeff", weightCoeff.getText());
        parameters.put("periodLength", steppingPeriodLength.getText());
        parameters.put("compositingPeriodLength", compositingPeriodLength.getText());
        parameters.put("resolution", resolution.getText());
        parameters.put("superSampling", superSampling.getText());
        return parameters;
    }

    public void setSelectedProcessor(DtoProcessorDescriptor selectedProcessor) {
        int selectedIndex = inputVariables.getSelectedIndex();
        String selectedItem = null;
        if (selectedIndex != -1) {
            selectedItem = inputVariables.getItemText(selectedIndex);
        }
        inputVariables.clear();
        DtoProcessorVariable[] processorVariables = selectedProcessor.getProcessorVariables();
        int newSelectedIndex = 0;
        int index = 0;
        for (DtoProcessorVariable processorVariable : processorVariables) {
            String processorVariableName = processorVariable.getName();
            inputVariables.addItem(processorVariableName);
            if (selectedIndex != -1 && processorVariableName.equals(selectedItem)) {
                newSelectedIndex = index;
            }
            index++;
        }
        inputVariables.setSelectedIndex(newSelectedIndex);
    }
}