/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoAggregatorDescriptor;
import com.bc.calvalus.portal.shared.DtoParameterDescriptor;
import com.google.gwt.cell.client.ButtonCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.SelectionCell;
import com.google.gwt.cell.client.TextCell;
import com.google.gwt.dom.client.Style;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The table for edditing the binning aggregators
 *
 * @author marcoz
 */
public class L3AggregatorTable {

    private static int lastId = 0;

    class ConfiguredAggregator {
        private final Integer id = ++lastId;
        private final Map<DtoParameterDescriptor, String> parameters = new HashMap<DtoParameterDescriptor, String>();
        private DtoAggregatorDescriptor aggregatorDescriptor;
        private ParametersEditorGenerator editor;

        public DtoAggregatorDescriptor getAggregatorDescriptor() {
            return aggregatorDescriptor;
        }

        public Map<DtoParameterDescriptor, String> getParameters() {
            return parameters;
        }

        public void setAggregatorDescriptor(DtoAggregatorDescriptor aggregatorDescriptor) {
            this.aggregatorDescriptor = aggregatorDescriptor;
            String title = "Edit '" + aggregatorDescriptor.getAggregator() + "' aggregator parameters";
            this.editor = new ParametersEditorGenerator(title, aggregatorDescriptor.getParameterDescriptors(), style);
        }

        private String getParametersAsText() {
            StringBuilder sb = new StringBuilder();
            DtoParameterDescriptor[] parameterDescriptors = aggregatorDescriptor.getParameterDescriptors();

            for (DtoParameterDescriptor parameterDescriptor : parameterDescriptors) {
                String name = parameterDescriptor.getName();
                if (parameters.containsKey(parameterDescriptor)) {
                    String value = parameters.get(parameterDescriptor);
                    String defaultValue = parameterDescriptor.getDefaultValue();
                    if (defaultValue == null || !defaultValue.equals(value)) {
                        sb.append(name);
                        sb.append("=");
                        sb.append(value);
                        sb.append("; ");
                    }
                }
            }
            if (sb.length() > 2) {
                sb.deleteCharAt(sb.length() - 2);
            }
            return sb.toString();
        }

        public Collection<String> getUsedVariables() {
            Set<String> usedVarNames = new HashSet<String>();
            for (DtoParameterDescriptor parameterDescriptor : aggregatorDescriptor.getParameterDescriptors()) {
                if (parameterDescriptor.getType().equals("variable")) {
                    String value = parameters.get(parameterDescriptor);
                    if (value != null) {
                        for (String s : value.split("\\,")) {
                            usedVarNames.add(s.trim());
                        }
                    }
                }
            }
            return usedVarNames;
        }

        public void fillParametersFromEditors() {
            parameters.clear();
            for (DtoParameterDescriptor parameterDescriptor : aggregatorDescriptor.getParameterDescriptors()) {
                parameters.put(parameterDescriptor, editor.getParameterValue(parameterDescriptor));
            }
        }
    }

    private final ListDataProvider<ConfiguredAggregator> dataProvider;
    private final SingleSelectionModel<ConfiguredAggregator> selectionModel;
    private final CellTable<ConfiguredAggregator> cellTable;

    private final List<DtoAggregatorDescriptor> aggregatorDescriptors;
    private CalvalusStyle style;
    private List<String> availableVariables;

    public L3AggregatorTable(List<DtoAggregatorDescriptor> aggregatorDescriptors) {
        this.aggregatorDescriptors = aggregatorDescriptors;

        ProvidesKey<ConfiguredAggregator> aggregatorKeyProvider = new ProvidesKey<ConfiguredAggregator>() {
            @Override
            public Object getKey(ConfiguredAggregator item) {
                return item.id;
            }
        };
        cellTable = new CellTable<ConfiguredAggregator>(aggregatorKeyProvider);

        // Add a selection model so we can select cells.
        selectionModel = new SingleSelectionModel<ConfiguredAggregator>(aggregatorKeyProvider);
        cellTable.setSelectionModel(selectionModel);

        // Initialize the columns.
        initAggregatorTableColumns();

        // Add the CellList to the adapter in the database.
        dataProvider = new ListDataProvider<ConfiguredAggregator>();
        dataProvider.addDataDisplay(cellTable);

        availableVariables = Collections.emptyList();
    }

    public void setStyle(CalvalusStyle style) {
        this.style = style;
    }

    public void setAvailableVariables(List<String> availableVariables) {
        this.availableVariables = availableVariables;
    }

    public CellTable<ConfiguredAggregator> getCellTable() {
        return cellTable;
    }

    public List<ConfiguredAggregator> getAggregatorList() {
        return dataProvider.getList();
    }

    public void addRow() {
        if (aggregatorDescriptors.size() > 0) {
            ConfiguredAggregator aggregator = new ConfiguredAggregator();
            aggregator.setAggregatorDescriptor(aggregatorDescriptors.get(0));
            aggregator.editor.setAvailableVariables(availableVariables);
            aggregator.fillParametersFromEditors();
            getAggregatorList().add(aggregator);
            dataProvider.refresh();
        }
    }

    public void removeSelectedRow() {
        ConfiguredAggregator selectedAggregator = selectionModel.getSelectedObject();
        if (selectedAggregator != null) {
            List<ConfiguredAggregator> list = getAggregatorList();
            list.remove(selectedAggregator);
            int remainingSize = list.size();
            if (remainingSize > 0) {
                selectionModel.setSelected(list.get(remainingSize), true);
            }
            dataProvider.refresh();
        }
    }

    private void initAggregatorTableColumns() {
        Column<ConfiguredAggregator, String> aggregatorColumn = createSelectAggregatorColumn();
        cellTable.addColumn(aggregatorColumn, "Aggregator");
        cellTable.setColumnWidth(aggregatorColumn, 10, Style.Unit.EM);

        Column<ConfiguredAggregator, String> editColumn = createEditButtonColumn();
        cellTable.addColumn(editColumn, "");
        cellTable.setColumnWidth(editColumn, 8, Style.Unit.EM);

        Column<ConfiguredAggregator, String> parameterColumn = createShowParameterColumn();
        cellTable.addColumn(parameterColumn, "Parameters");
        cellTable.setColumnWidth(parameterColumn, 44, Style.Unit.EM);
    }

    private Column<ConfiguredAggregator, String> createSelectAggregatorColumn() {
        List<String> aggregatorNameList = new ArrayList<String>();
        for (DtoAggregatorDescriptor aggregatorDescriptor : aggregatorDescriptors) {
            aggregatorNameList.add(aggregatorDisplayName(aggregatorDescriptor));
        }
        final SelectionCell aggregatorCell = new SelectionCell(aggregatorNameList);
        Column<ConfiguredAggregator, String> aggregatorColumn = new Column<ConfiguredAggregator, String>(aggregatorCell) {
            @Override
            public String getValue(ConfiguredAggregator aggregator) {
                return aggregator.aggregatorDescriptor.getAggregator();
            }
        };
        aggregatorColumn.setFieldUpdater(new FieldUpdater<ConfiguredAggregator, String>() {
            public void update(int index, ConfiguredAggregator configuredAggregator, String value) {
                if (aggregatorDisplayName(configuredAggregator.aggregatorDescriptor).equals(value)) {
                    return;
                }
                for (DtoAggregatorDescriptor aggregatorDescriptor : aggregatorDescriptors) {
                    if (aggregatorDisplayName(aggregatorDescriptor).equals(value)) {
                        configuredAggregator.setAggregatorDescriptor(aggregatorDescriptor);
                        showEditDialog(index, configuredAggregator);
                        dataProvider.refresh();
                        return;
                    }
                }
            }
        });
        return aggregatorColumn;
    }

    private static String aggregatorDisplayName(DtoAggregatorDescriptor aggregatorDescriptor) {
        String aggregatorName = aggregatorDescriptor.getAggregator();
        if (!aggregatorDescriptor.getOwner().isEmpty()) {
            return "(by " + aggregatorDescriptor.getOwner() + ") "  + aggregatorName;
        }
        return aggregatorName;
    }

    private Column<ConfiguredAggregator, String> createShowParameterColumn() {
        return new Column<ConfiguredAggregator, String>(new TextCell()) {
            @Override
            public String getValue(ConfiguredAggregator configuredAggregator) {
                return configuredAggregator.getParametersAsText();
            }
        };
    }

    private Column<ConfiguredAggregator, String> createEditButtonColumn() {
        Column<ConfiguredAggregator, String> column = new Column<ConfiguredAggregator, String>(new ButtonCell()) {
            @Override
            public String getValue(ConfiguredAggregator aggregator) {
                return "Edit";
            }
        };
        column.setFieldUpdater(new FieldUpdater<ConfiguredAggregator, String>() {
            @Override
            public void update(final int index, final ConfiguredAggregator aggregator, String value) {
                showEditDialog(index, aggregator);
            }
        });
        return column;
    }

    private void showEditDialog(final int index, final ConfiguredAggregator aggregator) {
        aggregator.editor.setAvailableVariables(availableVariables);
        String description = aggregator.aggregatorDescriptor.getDescriptionHtml();
        aggregator.editor.showDialog("600px", "320px", description, new ParametersEditorGenerator.OnOkHandler() {
            @Override
            public void onOk() {
                aggregator.fillParametersFromEditors();
                cellTable.redrawRow(index);
            }
        });
    }
}
