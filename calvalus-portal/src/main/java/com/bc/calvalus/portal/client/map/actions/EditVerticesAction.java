/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.portal.client.map.actions;

import com.bc.calvalus.portal.client.Dialog;
import com.bc.calvalus.portal.client.map.AbstractMapAction;
import com.bc.calvalus.portal.client.map.Region;
import com.bc.calvalus.portal.client.map.RegionMap;
import com.google.gwt.cell.client.EditTextCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.dom.client.Style;
import com.google.gwt.maps.client.geom.LatLng;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.view.client.ListDataProvider;

import java.util.List;

/**
 * Displays the vertices in a table and allows precise editing
 *
 * @author MarcoZ
 */
public class EditVerticesAction extends AbstractMapAction {

    private static final String TITLE = "Edit vertices";

    public EditVerticesAction() {
        super("E", TITLE);
    }

    @Override
    public void run(final RegionMap regionMap) {
        final Region selectedRegion = regionMap.getRegionMapSelectionModel().getSelectedRegion();
        if (selectedRegion == null) {
            Dialog.info(TITLE, "No region selected.");
            return;
        }
        if (!selectedRegion.isUserRegion()) {
            Dialog.info(TITLE, "You can only edit your own regions.");
            return;
        }
        final VerticesTable verticesTable = new VerticesTable(selectedRegion);
        Dialog dialog = new Dialog(TITLE,
                                   verticesTable,
                                   Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
            @Override
            protected void onOk() {
                selectedRegion.setVertices(verticesTable.getVertices());
                regionMap.getRegionModel().fireRegionChanged(null, selectedRegion);
                super.onOk();
            }
        };
        dialog.show();
    }

    private static class VerticesTable extends Composite {
        private final ListDataProvider<LatitudeLongitude> verticesProvider;
        private final CellTable<LatitudeLongitude> verticesTable;

        VerticesTable(Region selectedRegion) {
            verticesProvider = new ListDataProvider<LatitudeLongitude>();
            LatLng[] vertices = selectedRegion.getVertices();
            List<LatitudeLongitude> verticesProviderList = verticesProvider.getList();
            for (LatLng vertice : vertices) {
                LatitudeLongitude latitudeLongitude = new LatitudeLongitude(vertice.getLatitude(), vertice.getLongitude());
                verticesProviderList.add(latitudeLongitude);
            }
            // remove the last vertex, its equal to the first one
            verticesProviderList.remove(verticesProviderList.size()-1);
            verticesTable = new CellTable<LatitudeLongitude>();
            initTableColumns();
            verticesProvider.addDataDisplay(verticesTable);
            initWidget(verticesTable);
        }

        public LatLng[] getVertices() {
            List<LatitudeLongitude> verticesProviderList = verticesProvider.getList();
            LatLng[] vertices = new LatLng[verticesProviderList.size()+1];
            for (int i = 0; i < verticesProviderList.size(); i++) {
                LatitudeLongitude latitudeLongitude = verticesProviderList.get(i);
                vertices[i] = LatLng.newInstance(latitudeLongitude.latitude, latitudeLongitude.longitude);
            }
            LatitudeLongitude latitudeLongitude = verticesProviderList.get(0);
            vertices[vertices.length - 1] = LatLng.newInstance(latitudeLongitude.latitude, latitudeLongitude.longitude);
            return vertices;
        }

        private void initTableColumns() {
            Column<LatitudeLongitude, String> latColumn = createLatColumn();
            verticesTable.addColumn(latColumn, "Latitude");
            verticesTable.setColumnWidth(latColumn, 14, Style.Unit.EM);

            Column<LatitudeLongitude, String> lonColumn = createLonColumn();
            verticesTable.addColumn(lonColumn, "Longitude");
            verticesTable.setColumnWidth(lonColumn, 14, Style.Unit.EM);
        }

        private Column<LatitudeLongitude, String> createLatColumn() {
            Column<LatitudeLongitude, String> column = new Column<LatitudeLongitude, String>(new EditTextCell()) {
                @Override
                public String getValue(LatitudeLongitude object) {
                    return object.latitude + "";
                }
            };
            column.setFieldUpdater(new FieldUpdater<LatitudeLongitude, String>() {
                public void update(int index, LatitudeLongitude object, String value) {
                    object.latitude = Double.parseDouble(value);
                    verticesProvider.refresh();
                }
            });
            return column;
        }
        private Column<LatitudeLongitude, String> createLonColumn() {
            Column<LatitudeLongitude, String> column = new Column<LatitudeLongitude, String>(new EditTextCell()) {
                @Override
                public String getValue(LatitudeLongitude object) {
                    return object.longitude + "";
                }
            };
            column.setFieldUpdater(new FieldUpdater<LatitudeLongitude, String>() {
                public void update(int index, LatitudeLongitude object, String value) {
                    object.longitude = Double.parseDouble(value);
                    verticesProvider.refresh();
                }
            });
            return column;
        }

    }

    private static final class LatitudeLongitude {
        double latitude;
        double longitude;

        LatitudeLongitude(double latitude, double longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        @Override
        public String toString() {
            return "LatitudeLongitude{" +
                    "latitude=" + latitude +
                    ", longitude=" + longitude +
                    '}';
        }
    }
}
