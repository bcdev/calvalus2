package com.bc.calvalus.portal.client;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.shared.DtoAggregatorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.cell.client.Cell;
import com.google.gwt.cell.client.EditTextCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.SelectionCell;
import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Element;
import com.google.gwt.dom.client.NativeEvent;
import com.google.gwt.dom.client.Style;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.maps.client.base.LatLng;
import com.google.gwt.maps.client.base.LatLngBounds;
import com.google.gwt.maps.client.overlays.Polygon;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.bc.calvalus.portal.client.L3ConfigUtils.getPeriodCount;
import static com.bc.calvalus.portal.client.L3ConfigUtils.getTargetSizeEstimation;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Norman
 */
public class L3ConfigForm extends Composite {

    private static final DtoProcessorVariable EXPRESSION = new DtoProcessorVariable("<expression>", "AVG", "1.0");
    public static final List<String> DEFAULT_AGGREGATOR_NAMES = Arrays.asList(new String[]{
            "AVG",
            "MIN_MAX",
            "PERCENTILE_2",
            "PERCENTILE_5",
            "PERCENTILE_10",
            "PERCENTILE_25",
            "PERCENTILE_50",
            "PERCENTILE_75",
            "PERCENTILE_90",
            "PERCENTILE_95",
            "PERCENTILE_98"
    });
    public static final String COMPOSITING_TYPE_BINNING = "BINNING";
    public static final String COMPOSITING_TYPE_MOSAICKING = "MOSAICKING";

    private final PortalContext portalContext;
    private ListDataProvider<Variable> variableProvider;
    private SingleSelectionModel<Variable> variableSelectionModel;
    private DynamicSelectionCell variableNameCell;
    private LatLngBounds regionBounds;
    private final Map<String, DtoProcessorVariable> processorVariableDefaults;
    private final List<String> variableNames;
    private final List<String> aggregatorNameList;
    private final List<DtoAggregatorDescriptor> aggregatorDescriptors;


    interface TheUiBinder extends UiBinder<Widget, L3ConfigForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    public static class Variable {

        static int lastId = 0;
        Integer id = ++lastId;
        String name = "";
        String expression = "";
        String aggregator = "AVG";
        Double fillValue = Double.NaN;
        Double weightCoeff = 1.0;
        String parameter = "";
        String description = "";
    }


    @UiField
    TextBox maskExpr;
    @UiField
    IntegerBox steppingPeriodLength;
    @UiField
    IntegerBox compositingPeriodLength;
    @UiField
    IntegerBox periodCount;

    @UiField
    ListBox compositingType;
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
    @UiField
    HTML aggregatorDescriptionHTML;
    @UiField
    Anchor showL3ParametersHelp;

    private Date minDate;
    private Date maxDate;

    public L3ConfigForm(PortalContext portalContext) {
        this(portalContext, new ArrayList<>(DEFAULT_AGGREGATOR_NAMES));
    }

    public L3ConfigForm(PortalContext portalContext, List<String> aggregatorNameList) {
        this.portalContext = portalContext;
        processorVariableDefaults = new HashMap<String, DtoProcessorVariable>();
        variableNames = new ArrayList<String>();
        variableProvider = new ListDataProvider<Variable>();
        this.aggregatorNameList = aggregatorNameList;
        this.aggregatorDescriptors = new ArrayList<DtoAggregatorDescriptor>();
        if (portalContext != null) {
            retrieveAggregatorDescriptors();
        }

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

        ValueChangeHandler<Integer> periodCountUpdater = new ValueChangeHandler<Integer>() {
            @Override
            public void onValueChange(ValueChangeEvent<Integer> event) {
                updatePeriodCount();
            }
        };

        steppingPeriodLength.setValue(10);
        steppingPeriodLength.addValueChangeHandler(periodCountUpdater);

        compositingPeriodLength.setValue(10);
        compositingPeriodLength.addValueChangeHandler(periodCountUpdater);

        periodCount.setValue(0);
        periodCount.setEnabled(false);

        superSampling.setValue(1);

        resolution.setValue(9.28);
        resolution.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateTargetSize();
            }
        });
        compositingType.addItem(COMPOSITING_TYPE_BINNING);
        compositingType.addItem(COMPOSITING_TYPE_MOSAICKING);
        compositingType.setSelectedIndex(0);

        targetWidth.setEnabled(false);
        targetHeight.setEnabled(false);

        updateSpatialParameters(null);

        HelpSystem.addClickHandler(showL3ParametersHelp, "l3Parameters");
    }

    private void retrieveAggregatorDescriptors() {
        aggregatorDescriptors.clear();
        if (true) {
            Collections.addAll(aggregatorDescriptors, portalContext.getAggregators(BundleFilter.PROVIDER_SYSTEM));
        }
        if (true) {
            Collections.addAll(aggregatorDescriptors, portalContext.getAggregators(BundleFilter.PROVIDER_USER));
        }
        if (true) {
            Collections.addAll(aggregatorDescriptors, portalContext.getAggregators(BundleFilter.PROVIDER_ALL_USERS));
        }

        final Iterator<DtoAggregatorDescriptor> iterator = aggregatorDescriptors.iterator();
        while (iterator.hasNext()) {
            DtoAggregatorDescriptor aggregatorDescriptor = iterator.next();
            if (BundleFilter.DUMMY_AGGREGATOR_NAME.equals(aggregatorDescriptor.getAggregator())) {
                iterator.remove();
            }
        }
        for (DtoAggregatorDescriptor descriptor : aggregatorDescriptors) {
            aggregatorNameList.add(descriptor.getAggregator());
        }
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
            Polygon polygon = selectedRegion.createPolygon();
            regionBounds = Region.getBounds(polygon);
        } else {
            final LatLng sw = LatLng.newInstance(-90, -180);
            final LatLng ne = LatLng.newInstance(90, 180);
            regionBounds = LatLngBounds.newInstance(sw, ne);
        }
        updateTargetSize();
    }

    private void updateTargetSize() {
        int[] targetSize = getTargetSizeEstimation(regionBounds, resolution.getValue());
        targetWidth.setValue(targetSize[0]);
        targetHeight.setValue(targetSize[1]);
    }

    public void setProcessorDescriptor(DtoProcessorDescriptor processor, DtoProductSet productSet) {
        processorVariableDefaults.clear();
        variableNames.clear();
        variableNameCell.removeAllOptions();

        String defaultValidMask = "true";
        DtoProcessorVariable[] processorVariables;
        if (processor != null) {
            processorVariables = processor.getProcessorVariables();
            String defaultMaskExpression = processor.getDefaultMaskExpression();
            if (defaultMaskExpression != null) {
                defaultValidMask = defaultMaskExpression;
            }
        } else {
            String[] bandNames = productSet.getBandNames();
            processorVariables = new DtoProcessorVariable[bandNames.length];
            for (int i = 0; i < processorVariables.length; i++) {
                processorVariables[i] = new DtoProcessorVariable(bandNames[i], "AVG", "1.0");
            }
        }

        processorVariableDefaults.put(EXPRESSION.getName(), EXPRESSION);
        variableNames.add(EXPRESSION.getName());

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

        variableNameCell.addOptions(variableNames);
        if (variableList.size() == 0) {
            variableList.add(createDefaultVariable());
        }
        maskExpr.setValue(defaultValidMask);

        variableProvider.refresh();
    }

    public void validateForm() throws ValidationException {
        boolean periodCountValid = periodCount.getValue() != null && periodCount.getValue() >= 1;
        if (!periodCountValid) {
            throw new ValidationException(periodCount, "Period count must be >= 1");
        }

        Integer steppingP = steppingPeriodLength.getValue();
        boolean periodLengthValid = steppingP != null && (steppingP >= 1 || steppingP == -7 || steppingP == -30);
        if (!periodLengthValid) {
            throw new ValidationException(steppingPeriodLength, "Period length must be >= 1");
        }

        Integer compositingP = compositingPeriodLength.getValue();
        boolean compositingPeriodLengthValid = compositingP != null &&
                                               (compositingP >= 1 || compositingP == -7 || compositingP == -30);
        if (!compositingPeriodLengthValid) {
            throw new ValidationException(compositingPeriodLength,
                                          "Compositing period length must be >= 1");
        }

        boolean resolutionValid = resolution.getValue() != null && resolution.getValue() > 0.0;
        if (!resolutionValid) {
            throw new ValidationException(resolution, "Resolution must greater than zero");
        }

        Integer superSamplingValue = superSampling.getValue();
        boolean superSamplingValid = superSamplingValue != null && superSamplingValue >= 1 && superSamplingValue <= 9;
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
            if (variable.name.equals(EXPRESSION.getName())) {
                if (variable.expression.trim().isEmpty()) {
                    String message = "Expression must not be empty";
                    throw new ValidationException(variableTable, message);
                }
            }
        }

        boolean maskExprValid = !maskExpr.getValue().trim().isEmpty();
        if (!maskExprValid) {
            throw new ValidationException(maskExpr, "Good-pixel expression must not be empty ");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        List<Variable> variables = variableProvider.getList();
        int variablesLength = variables.size();
        int expressionCount = 0;
        parameters.put("variables.count", variables.size() + "");
        for (int i = 0; i < variablesLength; i++) {
            Variable variable = variables.get(i);
            String prefix = "variables." + i;
            if (variable.name.equals(EXPRESSION.getName())) {
                if (variable.expression.contains("=")) {
                    String[] split = variable.expression.split("=");
                    String variableName = split[0].trim();
                    String expression = split[1].trim();
                    parameters.put("expression." + expressionCount + ".variable", variableName);
                    parameters.put("expression." + expressionCount + ".expression", expression);
                    parameters.put(prefix + ".name", variableName);
                    expressionCount++;
                } else {
                    parameters.put(prefix + ".name", variable.expression);
                }
            } else {
                parameters.put(prefix + ".name", variable.name);
            }
            String aggregator = variable.aggregator;
            DtoAggregatorDescriptor aggregatorDescriptor = findDescriptor(aggregator);
            if (aggregator.startsWith("PERCENTILE_")) {
                parameters.put(prefix + ".aggregator", "PERCENTILE");
                parameters.put(prefix + ".percentage", aggregator.substring("PERCENTILE_".length()));
            } else if (aggregatorDescriptor != null) {
                parameters.put(prefix + ".aggregator", aggregator);
                parameters.put(prefix + "." + aggregatorDescriptor.getVariable(),
                               parameters.get(prefix + ".name"));
                parameters.put(prefix + ".descriptorVariableName", aggregatorDescriptor.getVariable());
                parameters.put(prefix + ".descriptorParameterName", aggregatorDescriptor.getParameter());
                if (aggregatorDescriptor.getParameter() != null && variable.parameter.length() > 0) {
                    parameters.put(prefix + "." + aggregatorDescriptor.getParameter(), variable.parameter);
                }
            } else {
                parameters.put(prefix + ".aggregator", aggregator);
            }
            parameters.put(prefix + ".weightCoeff", variable.weightCoeff + "");
            parameters.put(prefix + ".fillValue", variable.fillValue + "");
            // add aggregator bundle to bundles
            if (aggregatorDescriptor != null) {
                final String bundle;
                if (aggregatorDescriptor.getBundleLocation() != null) {
                    bundle = aggregatorDescriptor.getBundleLocation();
                } else if (aggregatorDescriptor.getBundleName() != null && aggregatorDescriptor.getBundleVersion() != null) {
                    bundle = aggregatorDescriptor.getBundleName() + "-" + aggregatorDescriptor.getBundleVersion();
                } else {
                    bundle = null;
                }
                if (bundle != null) {
                    if (parameters.containsKey("processorBundles")) {
                        final String bundles = parameters.get("processorBundles");
                        if (! bundles.contains(bundle)) {
                            parameters.put("processorBundles", bundles + "," + bundle);
                        }
                    } else {
                        parameters.put("processorBundles", bundle);
                    }
                }
            }
        }
        if (expressionCount > 0) {
            parameters.put("expression.count", expressionCount + "");
        }
        parameters.put("maskExpr", maskExpr.getText());
        parameters.put("periodLength", steppingPeriodLength.getValue().toString());
        parameters.put("compositingPeriodLength", compositingPeriodLength.getValue().toString());
        parameters.put("compositingType", getCompositingType());
        if (COMPOSITING_TYPE_MOSAICKING.equals(getCompositingType())) {
            parameters.put("planetaryGrid", "org.esa.beam.binning.support.PlateCarreeGrid");
        }
        parameters.put("resolution", resolution.getValue().toString());
        parameters.put("superSampling", superSampling.getValue().toString());
        return parameters;
    }

    private DtoAggregatorDescriptor findDescriptor(String aggregator) {
        for (DtoAggregatorDescriptor descriptor : aggregatorDescriptors) {
            if (aggregator.equals(descriptor.getAggregator())) {
                return descriptor;
            }
        }
        return null;
    }

    private void initTableColumns() {
        Column<Variable, String> nameColumn = createNameColumn();
        variableTable.addColumn(nameColumn, "Variable");
        variableTable.setColumnWidth(nameColumn, 14, Style.Unit.EM);

        Column<Variable, String> expressionColumn = createExpressionColumn();
        variableTable.addColumn(expressionColumn, "Expression");
        variableTable.setColumnWidth(expressionColumn, 14, Style.Unit.EM);

        Column<Variable, String> aggregatorColumn = createAggregatorColumn();
        variableTable.addColumn(aggregatorColumn, "Aggregator");
        variableTable.setColumnWidth(aggregatorColumn, 10, Style.Unit.EM);

        Column<Variable, String> weightCoeffColumn = createWeightCoeffColumn();
        variableTable.addColumn(weightCoeffColumn, "Weight");
        variableTable.setColumnWidth(weightCoeffColumn, 8, Style.Unit.EM);

        Column<Variable, String> fillValueColumn = createFillValueColumn();
        variableTable.addColumn(fillValueColumn, "Fill");
        variableTable.setColumnWidth(fillValueColumn, 8, Style.Unit.EM);

        Column<Variable, String> parameterColumn = createParameterColumn();
        variableTable.addColumn(parameterColumn, "Aggregator Parameter");
        variableTable.setColumnWidth(parameterColumn, 8, Style.Unit.EM);
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
                if (!value.equals(EXPRESSION.getName())) {
                    variable.expression = "";
                }
                variableProvider.refresh();
            }
        });
        return nameColumn;
    }

    private Column<Variable, String> createExpressionColumn() {
        Column<Variable, String> expressionColumn = new Column<Variable, String>(new EditTextCell()) {
            @Override
            public String getValue(Variable variable) {
                return variable.expression;
            }

            @Override
            public void onBrowserEvent(Cell.Context context, Element elem, Variable variable, NativeEvent event) {
                if (isEditable(variable)) {
                    super.onBrowserEvent(context, elem, variable, event);
                }
            }

            private boolean isEditable(Variable variable) {
                return variable.name.equals(EXPRESSION.getName());
            }

        };
        expressionColumn.setFieldUpdater(new FieldUpdater<Variable, String>() {
            public void update(int index, Variable variable, String value) {
                variable.expression = value;
                variableProvider.refresh();
            }
        });
        return expressionColumn;
    }

    private Column<Variable, String> createAggregatorColumn() {

        final SelectionCell aggregatorCell = new SelectionCell(aggregatorNameList);

        Column<Variable, String> aggregatorColumn = new Column<Variable, String>(aggregatorCell) {
            @Override
            public String getValue(Variable variable) {
                return variable.aggregator;
            }
        };
        aggregatorColumn.setFieldUpdater(new FieldUpdater<Variable, String>() {
            public void update(int index, Variable variable, String value) {
                variable.aggregator = value;
                aggregatorDescriptionHTML.setHTML(getAggregatorDescription(variable.aggregator));
                variableProvider.refresh();
            }
        });
        return aggregatorColumn;
    }

    private String getAggregatorDescription(String aggregatorName) {
        for (DtoAggregatorDescriptor aggregatorDescriptor: aggregatorDescriptors) {
            if (aggregatorName.equals(aggregatorDescriptor.getAggregator())) {
                return aggregatorDescriptor.getDescriptionHtml();
            }
        }
        return "AVG etc.: select variable, or type in an expression name=expr in the Expression column";
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

    private Column<Variable, String> createParameterColumn() {
        Column<Variable, String> parameterColumn = new Column<Variable, String>(new EditTextCell()) {
            @Override
            public String getValue(Variable object) {
                return object.parameter + "";
            }
        };
        parameterColumn.setFieldUpdater(new FieldUpdater<Variable, String>() {
            public void update(int index, Variable object, String value) {
                object.parameter = value;
                variableProvider.refresh();
            }
        });
        return parameterColumn;
    }

    private String getCompositingType() {
        int index = compositingType.getSelectedIndex();
        return compositingType.getValue(index);
    }
}