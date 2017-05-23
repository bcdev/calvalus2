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

import com.bc.calvalus.portal.shared.DtoParameterDescriptor;
import com.google.gwt.cell.client.ButtonCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.TextCell;
import com.google.gwt.dom.client.Style;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.client.ui.CheckBox;
import com.google.gwt.user.client.ui.FocusWidget;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.Collections;
import java.util.List;

/**
 * The table for editing the bands for region analysis
 *
 * @author marcoz
 */
public class RABandTable {

    public static final int MAX_NUM_BANDS = 100;
    private static int lastId = 0;
    private DtoParameterDescriptor[] desc;
    private ParametersEditorGenerator parametersEditor;


    static class ConfiguredBand {
        private final Integer id = ++lastId;
        private String name;
        private boolean histo;
        private String numBins;
        private String min;
        private String max;

        public ConfiguredBand() {
            this(null);
        }

        public ConfiguredBand(String name) {
            this(name, false, "100", "0.0", "1.0");
        }
        public ConfiguredBand(String name, boolean histo, String numBins, String min, String max) {
            this.name = name;
            this.histo = histo;
            this.numBins = numBins;
            this.min = min;
            this.max = max;
        }

        public String getName() {
            return name;
        }

        public boolean hasHistogram() {
            return histo;
        }

        public String getNumBins() {
            return numBins;
        }

        public String getMin() {
            return min;
        }

        public String getMax() {
            return max;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private final CellTable<ConfiguredBand> cellTable;
    private final SingleSelectionModel<ConfiguredBand> selectionModel;
    private final ListDataProvider<ConfiguredBand> dataProvider;
    private List<String> availableVariables;

    public RABandTable() {
        ProvidesKey<ConfiguredBand> keyProvider = new ProvidesKey<ConfiguredBand>() {
            @Override
            public Object getKey(ConfiguredBand item) {
                return item.id;
            }
        };
        cellTable = new CellTable<ConfiguredBand>(MAX_NUM_BANDS, keyProvider);
        cellTable.setWidth("100%", true);

        // Add a selection model so we can select cells.
        selectionModel = new SingleSelectionModel<ConfiguredBand>(keyProvider);
        cellTable.setSelectionModel(selectionModel);

        // Initialize the columns.
        initTableColumns();

        // Add the CellList to the adapter in the database.
        dataProvider = new ListDataProvider<ConfiguredBand>();
        dataProvider.addDataDisplay(cellTable);

        availableVariables = Collections.emptyList();
    }

    public void initStyle(CalvalusStyle style) {
        createParametersEditorGenerator(style);
    }

    public CellTable<ConfiguredBand> getCellTable() {
        return cellTable;
    }

    public List<ConfiguredBand> getBandList() {
        return dataProvider.getList();
    }

    public void addRow() {
        ConfiguredBand configuredBand = new ConfiguredBand();
        showEditDialog(configuredBand, new ParametersEditorGenerator.OnOkHandler() {
            @Override
            public boolean onOk() {
                if (configuredBand.name != null) {
                    getBandList().add(configuredBand);
                    dataProvider.refresh();
                }
                return true;
            }
        });
    }

    public void removeSelectedRow() {
        ConfiguredBand selectedVariable = selectionModel.getSelectedObject();
        if (selectedVariable != null) {
            List<ConfiguredBand> list = getBandList();
            list.remove(selectedVariable);
            int remainingSize = list.size();
            if (remainingSize > 0) {
                selectionModel.setSelected(list.get(remainingSize - 1), true);
            }
            dataProvider.refresh();
        }
    }
    
    public void refresh() {
        dataProvider.refresh();  
    }

    public boolean hasSelection() {
        return selectionModel.getSelectedObject() != null;
    }

    public HandlerRegistration addSelectionChangeHandler(SelectionChangeEvent.Handler hadler) {
        return selectionModel.addSelectionChangeHandler(hadler);
    }

    public void setAvailableVariables(List<String> availableVariables) {
        this.availableVariables = availableVariables;
    }

    private void initTableColumns() {
        Column<ConfiguredBand, String> nameColumn = createNameColumn();
        cellTable.addColumn(nameColumn, "Bands for Statistics");
        cellTable.setColumnWidth(nameColumn, 15, Style.Unit.EM);

        Column<ConfiguredBand, String> numBinsColumn = createNumBinsColumn();
        cellTable.addColumn(numBinsColumn, "#Histogram Bins");
        cellTable.setColumnWidth(numBinsColumn, 10, Style.Unit.EM);

        Column<ConfiguredBand, String> minColumn = createMinColumn();
        cellTable.addColumn(minColumn, "Min");
        cellTable.setColumnWidth(minColumn, 5, Style.Unit.EM);

        Column<ConfiguredBand, String> maxColumn = createMaxColumn();
        cellTable.addColumn(maxColumn, "Max");
        cellTable.setColumnWidth(maxColumn, 5, Style.Unit.EM);

        Column<ConfiguredBand, String> editColumn = createEditColumn();
        cellTable.addColumn(editColumn, "");
        cellTable.setColumnWidth(editColumn, 5, Style.Unit.EM);
    }

    private Column<ConfiguredBand, String> createNameColumn() {
        return new Column<ConfiguredBand, String>(new TextCell()) {
            @Override
            public String getValue(ConfiguredBand configuredVariable) {
                return configuredVariable.name;
            }
        };
    }

    private Column<ConfiguredBand, String> createNumBinsColumn() {
        return new Column<ConfiguredBand, String>(new TextCell()) {
            @Override
            public String getValue(ConfiguredBand cb) {
                return cb.histo ? cb.numBins : "";
            }
        };
    }

    private Column<ConfiguredBand, String> createMinColumn() {
        return new Column<ConfiguredBand, String>(new TextCell()) {
            @Override
            public String getValue(ConfiguredBand cb) {
                return cb.histo ? cb.min : "";
            }
        };
    }

    private Column<ConfiguredBand, String> createMaxColumn() {
        return new Column<ConfiguredBand, String>(new TextCell()) {
            @Override
            public String getValue(ConfiguredBand cb) {
                return cb.histo ? cb.max : "";
            }
        };
    }

    private Column<ConfiguredBand, String> createEditColumn() {
        Column<ConfiguredBand, String> column = new Column<ConfiguredBand, String>(new ButtonCell()) {
            @Override
            public String getValue(ConfiguredBand aggregator) {
                return "Edit";
            }
        };
        column.setFieldUpdater(new FieldUpdater<ConfiguredBand, String>() {
            @Override
            public void update(final int index, final ConfiguredBand configuredBand, String value) {
                showEditDialog(configuredBand, new ParametersEditorGenerator.OnOkHandler() {
                    @Override
                    public boolean onOk() {
                        cellTable.redrawRow(index);
                        return true;
                    }
                });
            }
        });
        return column;
    }

    private void createParametersEditorGenerator(CalvalusStyle style) {
        desc = new DtoParameterDescriptor[5];
        desc[0] = new DtoParameterDescriptor("Band name", "variable", "", "", null, null);
        desc[1] = new DtoParameterDescriptor("Histogram", "boolean", "Should a histogram be computed", "false", null, null);
        desc[2] = new DtoParameterDescriptor("#Bins", "int", "Number of histogram bins", "100", null, null);
        desc[3] = new DtoParameterDescriptor("Min", "float", "Minimum value of the histogram bins", "0.0", null, null);
        desc[4] = new DtoParameterDescriptor("Max", "float", "Maximum value of the histogram bins", "1.0", null, null);

        parametersEditor = new ParametersEditorGenerator("Configure Band Statistics", desc, style);

        CheckBox histoCheckbox = (CheckBox) parametersEditor.getWidget(desc[1]);
        ValueChangeHandler<Boolean> valueChangeHandler = new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
                setParametersEnablement(booleanValueChangeEvent.getValue());
            }
        };
        histoCheckbox.addValueChangeHandler(valueChangeHandler);
    }
    
    private void setParametersEnablement(boolean enabled) {
        ((FocusWidget) parametersEditor.getWidget(desc[2])).setEnabled(enabled);        
        ((FocusWidget) parametersEditor.getWidget(desc[3])).setEnabled(enabled);        
        ((FocusWidget) parametersEditor.getWidget(desc[4])).setEnabled(enabled);        
    }

    private void showEditDialog(final ConfiguredBand configuredBand, ParametersEditorGenerator.OnOkHandler onOkHandler) {
        parametersEditor.setAvailableVariables(availableVariables);

        parametersEditor.setParameterValue(desc[0], configuredBand.name);
        parametersEditor.setParameterValue(desc[1], Boolean.toString(configuredBand.histo));
        parametersEditor.setParameterValue(desc[2], configuredBand.numBins);
        parametersEditor.setParameterValue(desc[3], configuredBand.min);
        parametersEditor.setParameterValue(desc[4], configuredBand.max);
        setParametersEnablement(configuredBand.histo);

        parametersEditor.showDialog("600px", "320px", "", new ParametersEditorGenerator.OnOkHandler() {
            @Override
            public boolean onOk() {
                try {
                    configuredBand.name = parametersEditor.getParameterValue(desc[0]);
                    configuredBand.histo = Boolean.valueOf(parametersEditor.getParameterValue(desc[1]));
                    configuredBand.numBins = parametersEditor.getParameterValue(desc[2]);
                    configuredBand.min = parametersEditor.getParameterValue(desc[3]);
                    configuredBand.max = parametersEditor.getParameterValue(desc[4]);
                    onOkHandler.onOk();
                    return true;
                } catch (ValidationException e) {
                    e.handle();
                    return false;
                }
            }
        });
    }
}