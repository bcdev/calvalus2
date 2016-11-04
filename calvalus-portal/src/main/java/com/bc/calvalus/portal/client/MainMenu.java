/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.google.gwt.user.cellview.client.CellTree;
import com.google.gwt.user.cellview.client.HasKeyboardSelectionPolicy;
import com.google.gwt.user.cellview.client.TreeNode;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.SingleSelectionModel;

import java.util.List;

/**
 * The mein menu shown on the left hand side
 */
public class MainMenu {

    private final SingleSelectionModel<PortalView> selectionModel;
    private final MainMenuModel mainMenuModel;
    private final CellTree cellTree;

    MainMenu(List<PortalView> views) {
        selectionModel = new SingleSelectionModel<>();
        mainMenuModel = new MainMenuModel(views, selectionModel);
        // Create the cell tree.
        cellTree = new CellTree(mainMenuModel, null);
        cellTree.setAnimationEnabled(true);
        cellTree.setKeyboardSelectionPolicy(HasKeyboardSelectionPolicy.KeyboardSelectionPolicy.DISABLED);
        cellTree.setWidth("180px");
    }

    void showAndSelectFirstView() {
        TreeNode root = cellTree.getRootTreeNode();
        TreeNode category = root.setChildOpen(0, true);
        if (root.getChildCount() > 1) {
            root.setChildOpen(1, true);
        }
        PortalView portalView = (PortalView) category.getChildValue(0);
        selectionModel.setSelected(portalView, true);
    }

    void selectView(String id) {
        PortalView portalView = mainMenuModel.getView(id);
        MainMenuModel.Category category = mainMenuModel.getCategory(portalView);
        openCategory(category);
        selectionModel.setSelected(portalView, true);
    }

    private void openCategory(MainMenuModel.Category category) {
        TreeNode root = cellTree.getRootTreeNode();
        int childCount = root.getChildCount();
        for (int i = 0; i < childCount; i++) {
            if (root.getChildValue(i) == category) {
                root.setChildOpen(i, true, true);
                break;
            }
        }
    }

    SingleSelectionModel<PortalView> getSelectionModel() {
        return selectionModel;
    }

    Widget getWidget() {
        return cellTree;
    }
}
