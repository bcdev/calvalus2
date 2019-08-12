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

import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.cell.client.TextCell;
import com.google.gwt.cell.client.TextInputCell;
import com.google.gwt.dom.client.Style;
import com.google.gwt.event.logical.shared.HasValueChangeHandlers;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.event.shared.GwtEvent;
import com.google.gwt.event.shared.HandlerManager;
import com.google.gwt.event.shared.HandlerRegistration;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionChangeEvent;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.List;

/**
 * The table for editing the binning variables
 *
 * @author marcoz
 */
public class L3VariableTable implements HasValueChangeHandlers<L3VariableTable.ConfiguredVariable> {

    public static final int MAX_NUM_VARIABLES = 100;
    private static int lastId = 0;

    class ConfiguredVariable {
        private final Integer id = ++lastId;
        private String name;
        private String expression;

        public ConfiguredVariable() {
            this("", "");
        }

        public ConfiguredVariable(String name, String expr) {
            this.name = name;
            this.expression = expr;
        }

        public String getName() {
            return name;
        }

        public String getExpression() {
            return expression;
        }
    }

    private final CellTable<ConfiguredVariable> cellTable;
    private final SingleSelectionModel<ConfiguredVariable> selectionModel;
    private final ListDataProvider<ConfiguredVariable> dataProvider;
    private final HandlerManager handlerManager;


    public L3VariableTable() {

        ProvidesKey<ConfiguredVariable> keyProvider = new ProvidesKey<ConfiguredVariable>() {
            @Override
            public Object getKey(ConfiguredVariable item) {
                return item.id;
            }
        };
        cellTable = new CellTable<ConfiguredVariable>(MAX_NUM_VARIABLES, keyProvider);

        // Add a selection model so we can select cells.
        selectionModel = new SingleSelectionModel<ConfiguredVariable>(keyProvider);
        cellTable.setSelectionModel(selectionModel);

        // Initialize the columns.
        initTableColumns();

        // Add the CellList to the adapter in the database.
        dataProvider = new ListDataProvider<ConfiguredVariable>();
        dataProvider.addDataDisplay(cellTable);

        handlerManager = new HandlerManager(this);
    }

    public void setStyle(CalvalusStyle style) {
        cellTable.getColumn(0).setCellStyleNames(style.variableName());
        cellTable.getColumn(2).setCellStyleNames(style.variableExpression());
    }
    
    public void addSelectionChangeHandler(SelectionChangeEvent.Handler handler) {
        selectionModel.addSelectionChangeHandler(handler);
    }
    
    public boolean hasSelection() {
        return selectionModel.getSelectedObject() != null;
    }

    public CellTable<ConfiguredVariable> getCellTable() {
        return cellTable;
    }

    public List<ConfiguredVariable> getVariableList() {
        return dataProvider.getList();
    }

    public void addRow() {
        getVariableList().add(new ConfiguredVariable());
        dataProvider.refresh();
    }

    public void addRow(String name, String expr) {
        getVariableList().add(new ConfiguredVariable(name, expr));
        dataProvider.refresh();
        VariableListChangeEvent.fire(L3VariableTable.this);
    }


    public void removeSelectedRow() {
        ConfiguredVariable selectedVariable = selectionModel.getSelectedObject();
        if (selectedVariable != null) {
            List<ConfiguredVariable> list = getVariableList();
            list.remove(selectedVariable);
            int remainingSize = list.size();
            if (remainingSize > 0) {
                selectionModel.setSelected(list.get(remainingSize - 1), true);
            }
            VariableListChangeEvent.fire(L3VariableTable.this);
            dataProvider.refresh();
        }
    }

    @Override
    public HandlerRegistration addValueChangeHandler(ValueChangeHandler<ConfiguredVariable> handler) {
        return handlerManager.addHandler(VariableListChangeEvent.getType(), handler);
    }

    @Override
    public void fireEvent(GwtEvent<?> event) {
        handlerManager.fireEvent(event);
    }

    private void initTableColumns() {
        Column<ConfiguredVariable, String> nameColumn = createNameColumn();
        cellTable.addColumn(nameColumn, "Variable Name");
        cellTable.setColumnWidth(nameColumn, 10, Style.Unit.EM);

        Column<ConfiguredVariable, String> equalsColumn = createEqualsColumn();
        cellTable.addColumn(equalsColumn, "");
        cellTable.setColumnWidth(equalsColumn, 3, Style.Unit.EM);

        Column<ConfiguredVariable, String> expressionColumn = createExpressionColumn();
        cellTable.addColumn(expressionColumn, "Expression");
        cellTable.setColumnWidth(expressionColumn, 49, Style.Unit.EM);
    }

    private Column<ConfiguredVariable, String> createNameColumn() {
        Column<ConfiguredVariable, String> column = new Column<ConfiguredVariable, String>(new TextInputCell()) {
            @Override
            public String getValue(ConfiguredVariable configuredVariable) {
                return configuredVariable.name;
            }
        };
        column.setFieldUpdater(new FieldUpdater<ConfiguredVariable, String>() {
            @Override
            public void update(final int index, final ConfiguredVariable configuredVariable, String value) {
                configuredVariable.name = value;
                dataProvider.refresh();
                VariableListChangeEvent.fire(L3VariableTable.this);
            }
        });
        return column;
    }

    private Column<ConfiguredVariable, String> createEqualsColumn() {
        return new Column<ConfiguredVariable, String>(new TextCell()) {
            @Override
            public String getValue(ConfiguredVariable configuredAggregator) {
                return "=";
            }
        };
    }

    private Column<ConfiguredVariable, String> createExpressionColumn() {
        Column<ConfiguredVariable, String> column = new Column<ConfiguredVariable, String>(new TextInputCell()) {
            @Override
            public String getValue(ConfiguredVariable configuredVariable) {
                return configuredVariable.expression;
            }
        };
        column.setFieldUpdater(new FieldUpdater<ConfiguredVariable, String>() {
            @Override
            public void update(final int index, final ConfiguredVariable configuredVariable, String value) {
                configuredVariable.expression = value;
                dataProvider.refresh();
            }
        });
        return column;
    }

    static class VariableListChangeEvent extends ValueChangeEvent<ConfiguredVariable> {

        public static <T> void fire(HasValueChangeHandlers<T> source) {
            source.fireEvent(new VariableListChangeEvent());
        }

        protected VariableListChangeEvent() {
            super(null);
        }
    }
}
