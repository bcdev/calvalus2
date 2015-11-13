package com.bc.calvalus.portal.client;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.shared.DtoAggregatorDescriptor;
import com.bc.calvalus.portal.shared.DtoParameterDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProcessorVariable;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.core.client.GWT;
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
import com.google.gwt.user.client.ui.Anchor;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.DoubleBox;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.bc.calvalus.portal.client.L3ConfigUtils.getPeriodCount;
import static com.bc.calvalus.portal.client.L3ConfigUtils.getTargetSizeEstimation;

/**
 * Demo view that lets users submit a new L3 production.
 *
 * @author Norman
 */
public class L3ConfigForm extends Composite {

    public static final String COMPOSITING_TYPE_BINNING = "BINNING";
    public static final String COMPOSITING_TYPE_MOSAICKING = "MOSAICKING";

    private final List<String> l3InputVarNames;
    private final L3AggregatorTable aggregatorTable;
    private final L3VariableTable variableTable;
    private LatLngBounds regionBounds;


    interface TheUiBinder extends UiBinder<Widget, L3ConfigForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);


    @UiField
    CalvalusStyle style;

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
    CellTable<L3VariableTable.ConfiguredVariable> variableCellTable;
    @UiField
    Button addVariableButton;
    @UiField
    Button removeVariableButton;

    @UiField(provided = true)
    CellTable<L3AggregatorTable.ConfiguredAggregator> aggregatorCellTable;
    @UiField
    Button addAggregatorButton;
    @UiField
    Button removeAggregatorButton;

    @UiField
    Anchor showL3ParametersHelp;

    private Date minDate;
    private Date maxDate;

    public L3ConfigForm(PortalContext portalContext, String... filterAggregatorNames) {
        this.l3InputVarNames = new ArrayList<String>();
        List<DtoAggregatorDescriptor> aggregatorDescriptors = retrieveAggregatorDescriptors(portalContext, filterAggregatorNames);

        variableTable = new L3VariableTable();
        variableCellTable = variableTable.getCellTable();

        aggregatorTable = new L3AggregatorTable(aggregatorDescriptors);
        aggregatorCellTable = aggregatorTable.getCellTable();

        initWidget(uiBinder.createAndBindUi(this));

        variableTable.setStyle(style);
        aggregatorTable.setStyle(style);

        addVariableButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                variableTable.addRow();
                removeVariableButton.setEnabled(variableTable.getVariableList().size() > 0);
            }
        });

        removeVariableButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                variableTable.removeSelectedRow();
                removeVariableButton.setEnabled(variableTable.getVariableList().size() > 0);
            }
        });
        removeVariableButton.setEnabled(false);

        addAggregatorButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                aggregatorTable.addRow();
                removeAggregatorButton.setEnabled(aggregatorTable.getAggregatorList().size() > 0);
            }
        });

        removeAggregatorButton.addClickHandler(new ClickHandler() {
            @Override
            public void onClick(ClickEvent event) {
                aggregatorTable.removeSelectedRow();
                removeAggregatorButton.setEnabled(aggregatorTable.getAggregatorList().size() > 0);
            }
        });

        variableTable.addValueChangeHandler(new ValueChangeHandler<L3VariableTable.ConfiguredVariable>() {
            @Override
            public void onValueChange(ValueChangeEvent<L3VariableTable.ConfiguredVariable> event) {
                updateAvailableVariables();
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

    private List<DtoAggregatorDescriptor> retrieveAggregatorDescriptors(PortalContext portalContext, String[] filterAggregatorNamesArray) {
        List<DtoAggregatorDescriptor> allAvailable = new ArrayList<DtoAggregatorDescriptor>();
        if (true) {
            Collections.addAll(allAvailable, portalContext.getAggregators(BundleFilter.PROVIDER_SYSTEM));
        }
        if (true) {
            Collections.addAll(allAvailable, portalContext.getAggregators(BundleFilter.PROVIDER_USER));
        }
        if (true) {
            Collections.addAll(allAvailable, portalContext.getAggregators(BundleFilter.PROVIDER_ALL_USERS));
        }

        List<DtoAggregatorDescriptor> aggregatorDescriptors = new ArrayList<DtoAggregatorDescriptor>();
        Set<String> usedAggregatorNames = new HashSet<String>();
        Set<String> filterAggregatorNames = new HashSet<String>(Arrays.asList(filterAggregatorNamesArray));

        for (DtoAggregatorDescriptor descriptor : allAvailable) {
            String aggregatorName = descriptor.getAggregator();
            if ((filterAggregatorNames.isEmpty() || filterAggregatorNames.contains(aggregatorName)) &&
                    !usedAggregatorNames.contains(aggregatorName)) {
                usedAggregatorNames.add(aggregatorName);
                aggregatorDescriptors.add(descriptor);
            }
        }
        return aggregatorDescriptors;
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

    public void updateAvailableVariables() {
        List<String> availableVariables = new ArrayList<String>(l3InputVarNames);
        for (L3VariableTable.ConfiguredVariable variable : variableTable.getVariableList()) {
            if (!variable.getName().isEmpty()) {
                availableVariables.add(variable.getName());
            }
        }
        aggregatorTable.setAvailableVariables(availableVariables);
    }

    public void setProcessorDescriptor(DtoProcessorDescriptor processor, DtoProductSet productSet) {
        l3InputVarNames.clear();

        String defaultValidMask = "true";
        if (processor != null) {
            for (DtoProcessorVariable variable : processor.getProcessorVariables()) {
                l3InputVarNames.add(variable.getName());
            }
            String defaultMaskExpression = processor.getDefaultMaskExpression();
            if (defaultMaskExpression != null) {
                defaultValidMask = defaultMaskExpression;
            }
        } else {
            String[] bandNames = productSet.getBandNames();
            Collections.addAll(l3InputVarNames, bandNames);
        }
        updateAvailableVariables();
        if (aggregatorTable.getAggregatorList().size() == 0) {
            aggregatorTable.addRow();
        }

        maskExpr.setValue(defaultValidMask);
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

        List<String> availableVariables = new ArrayList<String>(l3InputVarNames);
        List<L3VariableTable.ConfiguredVariable> variableList = variableTable.getVariableList();
        for (int i = 0; i < variableList.size(); i++) {
            L3VariableTable.ConfiguredVariable variable = variableList.get(i);
            if (variable.getName().isEmpty()) {
                throw new ValidationException(variableCellTable, "Variable number " + (i + 1) + ": Name must not be emtpy.");
            } else {
                availableVariables.add(variable.getName());
            }
        }

        List<L3AggregatorTable.ConfiguredAggregator> aggregatorList = aggregatorTable.getAggregatorList();
        for (int i = 0; i < aggregatorList.size(); i++) {
            L3AggregatorTable.ConfiguredAggregator aggregator = aggregatorList.get(i);
            Collection<String> usedVariables = aggregator.getUsedVariables();
            if (!availableVariables.containsAll(usedVariables)) {
                throw new ValidationException(aggregatorCellTable, "Aggregator number " + (i + 1) + ": References to a not existing variable.");
            }
            for (Map.Entry<DtoParameterDescriptor, String> entry : aggregator.getParameters().entrySet()) {
                DtoParameterDescriptor parameterDescriptor = entry.getKey();
                if (parameterDescriptor.getType().equals("float") || parameterDescriptor.getType().equals("int")) {
                    if (entry.getValue() == null) {
                        String msg = "Aggregator number " + (i + 1) + ": Not a valid value for parameter '" + parameterDescriptor.getName() + "'";
                        throw new ValidationException(aggregatorCellTable, msg);
                    }
                }
            }
        }
        if (aggregatorList.size() == 0) {
            throw new ValidationException(aggregatorCellTable, "At least one binning aggregator must be defined.");
        }

        boolean maskExprValid = !maskExpr.getValue().trim().isEmpty();
        if (!maskExprValid) {
            throw new ValidationException(maskExpr, "Good-pixel expression must not be empty ");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();

        List<L3AggregatorTable.ConfiguredAggregator> aggregatorList = aggregatorTable.getAggregatorList();
        int vIndex = 0;
        for (; vIndex < aggregatorList.size(); vIndex++) {
            L3AggregatorTable.ConfiguredAggregator aggregator = aggregatorList.get(vIndex);
            DtoAggregatorDescriptor aggregatorDescriptor = aggregator.getAggregatorDescriptor();
            final String prefix = "variables." + vIndex;
            parameters.put(prefix + ".aggregator", aggregatorDescriptor.getAggregator());
            Map<DtoParameterDescriptor, String> aggregatorParameters = aggregator.getParameters();
            DtoParameterDescriptor[] parameterDescriptors = aggregatorDescriptor.getParameterDescriptors();
            int pIndex = 0;
            for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
                String pName = parameterDescriptor.getName();
                if (aggregatorParameters.containsKey(parameterDescriptor)) {
                    String pValue = aggregatorParameters.get(parameterDescriptor);
                    String defaultValue = parameterDescriptor.getDefaultValue();
                    if (defaultValue == null || !pValue.equals(defaultValue)) {
                        parameters.put(prefix + ".parameter." + pIndex + ".name", pName);
                        parameters.put(prefix + ".parameter." + pIndex + ".value", pValue);
                        pIndex++;
                    }
                }
            }
            parameters.put(prefix + ".parameter.count", pIndex + "");

            // add aggregator bundle to bundles
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
                    if (!bundles.contains(bundle)) {
                        parameters.put("processorBundles", bundles + "," + bundle);
                    }
                } else {
                    parameters.put("processorBundles", bundle);
                }
            }
        }
        parameters.put("variables.count", vIndex + "");

        List<L3VariableTable.ConfiguredVariable> variableList = variableTable.getVariableList();
        int eIndex = 0;
        for (int i = 0; i < variableList.size(); i++) {
            L3VariableTable.ConfiguredVariable variable = variableList.get(i);
            if (!variable.getExpression().isEmpty()) {
                parameters.put("expression." + i + ".variable", variable.getName());
                parameters.put("expression." + i + ".expression", variable.getExpression());
                eIndex++;
            }
        }
        if (eIndex > 0) {
            parameters.put("expression.count", eIndex + "");
        }

        parameters.put("maskExpr", maskExpr.getText());
        parameters.put("periodLength", steppingPeriodLength.getValue().toString());
        parameters.put("compositingPeriodLength", compositingPeriodLength.getValue().toString());
        parameters.put("compositingType", getCompositingType());
        if (COMPOSITING_TYPE_MOSAICKING.equals(getCompositingType())) {
            parameters.put("planetaryGrid", "org.esa.snap.binning.support.PlateCarreeGrid");
        }
        parameters.put("resolution", resolution.getValue().toString());
        parameters.put("superSampling", superSampling.getValue().toString());
        return parameters;
    }

    private String getCompositingType() {
        int index = compositingType.getSelectedIndex();
        return compositingType.getValue(index);
    }
}