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
import com.google.gwt.user.client.ui.*;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.*;

import static com.bc.calvalus.portal.client.L3ConfigUtils.getPeriodCount;
import static com.bc.calvalus.portal.client.L3ConfigUtils.getTargetSizeEstimation;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class L3ConfigForm extends Composite {

    private ListDataProvider<Variable> variableProvider;
    private SingleSelectionModel<Variable> variableSelectionModel;
    private DynamicSelectionCell variableNameCell;
    private LatLngBounds regionBounds;
    private final Map<String, DtoProcessorVariable> processorVariableDefaults;
    private final List<String> variableNames;

    interface TheUiBinder extends UiBinder<Widget, L3ConfigForm> {
    }

    public static class Variable {
        static int lastId = 0;
        Integer id = ++lastId;
        String name = "";

        String aggregator = "AVG";
        Double fillValue = Double.NaN;
        Double weightCoeff = 1.0;
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    TextBox maskExpr;
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

    @UiField(provided = true)
    CellTable<Variable> variableTable;
    @UiField
    Button addVariableButton;
    @UiField
    Button removeVariableButton;

    private Date minDate;
    private Date maxDate;

    public L3ConfigForm() {
        processorVariableDefaults = new HashMap<String, DtoProcessorVariable>();
        variableNames = new ArrayList<String>();
        variableProvider = new ListDataProvider<Variable>();

        // Set a key provider that provides a unique key for each contact. If key is
        // used to identify contacts when fields (such as the name and address)
        // change.
        ProvidesKey<Variable> keyProvider = new ProvidesKey<Variable>() {
            @Override
            public Object getKey(Variable item) {
                return item.id;
            }
        };
        variableTable = new CellTable<Variable>(keyProvider);

        // Add a selection model so we can select cells.
        variableSelectionModel = new SingleSelectionModel<Variable>(keyProvider);
        variableTable.setSelectionModel(variableSelectionModel);

        // Initialize the columns.
        initTableColumns();

        // Add the CellList to the adapter in the database.
        variableProvider.addDataDisplay(variableTable);

        initWidget(uiBinder.createAndBindUi(this));

        addVariableButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                variableProvider.getList().add(createDefaultVariable());
                variableProvider.refresh();
            }
        });

        removeVariableButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                Variable selectedVariable = variableSelectionModel.getSelectedObject();
                if (selectedVariable != null) {
                    variableProvider.getList().remove(selectedVariable);
                    variableProvider.refresh();
                }
            }
        });

        steppingPeriodLength.setValue(10);
        steppingPeriodLength.addValueChangeHandler(new ValueChangeHandler<Integer>() {
            @Override
            public void onValueChange(ValueChangeEvent<Integer> event) {
                updatePeriodCount();
            }
        });

        resolution.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateTargetSize();
            }
        });

        compositingPeriodLength.setValue(10);

        periodCount.setValue(0);
        periodCount.setEnabled(false);

        superSampling.setValue(1);
        resolution.setValue(9.28);

        targetWidth.setEnabled(false);
        targetHeight.setEnabled(false);

        updateSpatialParameters(null);
    }

    private Variable createDefaultVariable() {
        Variable variable = new Variable();
        if (!variableNames.isEmpty()) {
            variable.name = variableNames.get(0);
            DtoProcessorVariable dtoProcessorVariable = processorVariableDefaults.get(variable.name);
            applyDefaultToVariable(dtoProcessorVariable, variable);
        }
        return variable;
    }

    private void applyDefaultToVariable(DtoProcessorVariable processorVariable, Variable variable) {
        String defaultAggregator = processorVariable.getDefaultAggregator();
        if (defaultAggregator != null) {
            variable.aggregator = defaultAggregator;
        }
        try {
            String defaultWeightCoeff = processorVariable.getDefaultWeightCoeff();
            if (defaultWeightCoeff != null) {
                variable.weightCoeff = Double.parseDouble(defaultWeightCoeff);
            }
        } catch (NumberFormatException e) {
            // the given coeff is neither given or not a number
        }
    }

    public void updateTemporalParameters(Date minDate, Date maxDate) {
        this.minDate = minDate;
        this.maxDate = maxDate;
        updatePeriodCount();
    }

    private void updatePeriodCount() {
        if (minDate != null && maxDate != null) {
            periodCount.setValue(getPeriodCount(minDate,
                                                maxDate,
                                                steppingPeriodLength.getValue(),
                                                compositingPeriodLength.getValue()));
        } else {
            periodCount.setValue(0);
        }
    }

    public void updateSpatialParameters(Region selectedRegion) {
        if (selectedRegion != null) {
            regionBounds = selectedRegion.createPolygon().getBounds();
        } else {
            regionBounds = LatLngBounds.newInstance(LatLng.newInstance(-90, -180),
                                                    LatLng.newInstance(90, 180));
        }
        updateTargetSize();
    }

    private void updateTargetSize() {
        int[] targetSize = getTargetSizeEstimation(regionBounds, resolution.getValue());
        targetWidth.setValue(targetSize[0]);
        targetHeight.setValue(targetSize[1]);
    }

    public void setProcessorDescriptor(DtoProcessorDescriptor selectedProcessor) {
        if (selectedProcessor == null) {
            return;
        }
        processorVariableDefaults.clear();
        variableNames.clear();
        DtoProcessorVariable[] processorVariables = selectedProcessor.getProcessorVariables();
        for (DtoProcessorVariable processorVariable : processorVariables) {
            String processorVariableName = processorVariable.getName();
            processorVariableDefaults.put(processorVariableName, processorVariable);
            variableNames.add(processorVariableName);
        }
        List<Variable> variableList = variableProvider.getList();
        Iterator<Variable> iterator = variableList.iterator();
        while (iterator.hasNext()) {
            Variable variable = iterator.next();
            String name = variable.name;
            if (!variableNames.contains(name)) {
                iterator.remove();
            }
        }
        variableNameCell.removeAllOptions();
        variableNameCell.addOptions(variableNames);
        if (variableList.size() == 0) {
            variableList.add(createDefaultVariable());
        }
        variableProvider.refresh();

        String defaultValidMask = selectedProcessor.getDefaultMaskExpr();
        if (defaultValidMask != null) {
            maskExpr.setValue(defaultValidMask);
        } else {
            maskExpr.setValue("");
        }

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

        boolean resolutionValid = resolution.getValue() > 0.0;
        if (!resolutionValid) {
            throw new ValidationException(resolution, "Resolution must greater than zero");
        }

        boolean superSamplingValid = superSampling.getValue() >= 1 && superSampling.getValue() <= 9;
        if (!superSamplingValid) {
            throw new ValidationException(superSampling, "Super-sampling must be >= 1 and <= 9");
        }

        List<Variable> variableList = variableProvider.getList();
        if (variableList.size() == 0) {
            throw new ValidationException(variableTable, "At least one binning variable must be defined.");
        }

        for (Variable variable : variableList) {
            boolean weightCoeffValid = variable.weightCoeff >= 0.0 && variable.weightCoeff <= 1.0;
            if (!weightCoeffValid) {
                String message = "Weight coefficient for '" + variable.name + "' must be >= 0 and <= 1";
                throw new ValidationException(variableTable, message);
            }
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        List<Variable> variables = variableProvider.getList();
        int variablesLength = variables.size();
        parameters.put("variables.count", variables.size() + "");
        for (int i = 0; i < variablesLength; i++) {
            Variable variable = variables.get(i);
            String prefix = "variables." + i;
            parameters.put(prefix + ".name", variable.name);
            parameters.put(prefix + ".aggregator", variable.aggregator);
            parameters.put(prefix + ".weightCoeff", variable.weightCoeff + "");
            parameters.put(prefix + ".fillValue", variable.fillValue + "");
        }
        parameters.put("maskExpr", maskExpr.getText());
        parameters.put("periodLength", steppingPeriodLength.getText());
        parameters.put("compositingPeriodLength", compositingPeriodLength.getText());
        parameters.put("resolution", resolution.getText());
        parameters.put("superSampling", superSampling.getText());
        return parameters;
    }


    private void initTableColumns() {
        Column<Variable, String> nameColumn = createNameColumn();
        variableTable.addColumn(nameColumn, "Variable");
        variableTable.setColumnWidth(nameColumn, 14, Style.Unit.EM);

        Column<Variable, String> aggregatorColumn = createAggregatorColumn();
        variableTable.addColumn(aggregatorColumn, "Aggregator");
        variableTable.setColumnWidth(aggregatorColumn, 10, Style.Unit.EM);

        Column<Variable, String> weightCoeffColumn = createWeightCoeffColumn();
        variableTable.addColumn(weightCoeffColumn, "Weight");
        variableTable.setColumnWidth(weightCoeffColumn, 8, Style.Unit.EM);

        Column<Variable, String> fillValueColumn = createFillValueColumn();
        variableTable.addColumn(fillValueColumn, "Fill");
        variableTable.setColumnWidth(fillValueColumn, 8, Style.Unit.EM);
    }

    private Column<Variable, String> createNameColumn() {
        variableNameCell = new DynamicSelectionCell(new ArrayList<String>());
        Column<Variable, String> nameColumn = new Column<Variable, String>(variableNameCell) {
            @Override
            public String getValue(Variable variable) {
                return variable.name;
            }
        };
        nameColumn.setFieldUpdater(new FieldUpdater<Variable, String>() {
            public void update(int index, Variable variable, String value) {
                variable.name = value;
                DtoProcessorVariable dtoProcessorVariable = processorVariableDefaults.get(value);
                if (dtoProcessorVariable != null) {
                    applyDefaultToVariable(dtoProcessorVariable, variable);
                }
                variableProvider.refresh();
            }
        });
        return nameColumn;
    }

    private Column<Variable, String> createAggregatorColumn() {

        List<String> valueList = Arrays.asList(
                "AVG",
                "AVG_ML",
                "MIN_MAX",
                "PERCENTILE"
        );

        SelectionCell aggregatorCell = new SelectionCell(valueList);
        Column<Variable, String> aggregatorColumn = new Column<Variable, String>(aggregatorCell) {
            @Override
            public String getValue(Variable variable) {
                return variable.aggregator;
            }
        };
        aggregatorColumn.setFieldUpdater(new FieldUpdater<Variable, String>() {
            public void update(int index, Variable variable, String value) {
                variable.aggregator = value;
                variableProvider.refresh();
            }
        });
        return aggregatorColumn;
    }

    private Column<Variable, String> createWeightCoeffColumn() {
        Column<Variable, String> fillValueColumn = new Column<Variable, String>(new EditTextCell()) {
            @Override
            public String getValue(Variable object) {
                return object.weightCoeff + "";
            }
        };
        fillValueColumn.setFieldUpdater(new FieldUpdater<Variable, String>() {
            public void update(int index, Variable object, String value) {
                object.weightCoeff = Double.parseDouble(value);
                variableProvider.refresh();
            }
        });
        return fillValueColumn;
    }

    private Column<Variable, String> createFillValueColumn() {
        Column<Variable, String> fillValueColumn = new Column<Variable, String>(new EditTextCell()) {
            @Override
            public String getValue(Variable object) {
                return object.fillValue + "";
            }
        };
        fillValueColumn.setFieldUpdater(new FieldUpdater<Variable, String>() {
            public void update(int index, Variable object, String value) {
                object.fillValue = Double.parseDouble(value);
                variableProvider.refresh();
            }
        });
        return fillValueColumn;
    }
}