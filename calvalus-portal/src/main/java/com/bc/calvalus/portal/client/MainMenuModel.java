/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.bc.calvalus.portal.client;

import com.google.gwt.cell.client.AbstractCell;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.SelectionModel;
import com.google.gwt.view.client.TreeViewModel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@link com.google.gwt.view.client.TreeViewModel} used by the main menu.
 */
public class MainMenuModel implements TreeViewModel {

    private final CalvalusPortal portal;

    /**
     * The top level categories.
     */
    private final ListDataProvider<Category> categories = new ListDataProvider<Category>();

    /**
     * A mapping of {@link PortalView}s to their associated categories.
     */
    private final Map<PortalView, Category> categoryMap = new HashMap<PortalView, Category>();

    /**
     * The cell used to render examples.
     */
    private final ViewCell viewCell = new ViewCell();

    /**
     * A mapping of history tokens to their associated {@link PortalView}.
     */
    private final Map<String, PortalView> viewMap = new HashMap<String, PortalView>();

    /**
     * The selection model used to select examples.
     */
    private final SelectionModel<PortalView> selectionModel;

    public MainMenuModel(CalvalusPortal portal, SelectionModel<PortalView> selectionModel) {
        this.portal = portal;
        this.selectionModel = selectionModel;
        initialize();
    }

    @Override
    public <T> NodeInfo<?> getNodeInfo(T value) {
        if (value == null) {
            // Return the top level categories.
            return new DefaultNodeInfo<Category>(categories, new CategoryCell());
        } else if (value instanceof Category) {
            // Return the examples within the category.
            Category category = (Category) value;
            return category.getNodeInfo();
        }
        return null;
    }

    @Override
    public boolean isLeaf(Object value) {
        return value != null && !(value instanceof Category);
    }

    /**
     * Get the {@link Category} associated with a view.
     *
     * @param view the {@link PortalView}
     * @return the associated {@link Category}
     */
    public Category getCategory(PortalView view) {
        return categoryMap.get(view);
    }

    /**
     * Get the view associated with the specified ID (e.g. history token).
     *
     * @param id the history token
     * @return the associated {@link PortalView}
     */
    public PortalView getView(String id) {
        return viewMap.get(id);
    }

    /**
     * Get the set of all {@link PortalView}s used in the model.
     *
     * @return the {@link PortalView}s
     */
    Set<PortalView> getViews() {
        Set<PortalView> widgets = new HashSet<PortalView>();
        for (Category category : categories.getList()) {
            for (PortalView example : category.items.getList()) {
                widgets.add(example);
            }
        }
        return widgets;
    }

    /**
     * Initialize the top level categories in the tree.
     */
    private void initialize() {
        List<Category> catList = categories.getList();

        // Production.
        {
            Category category = new Category("Production");
            catList.add(category);
            category.addItem(new ManageProductSetsView(portal));
            category.addItem(new OrderL2ProductionView(portal));
            category.addItem(new OrderL3ProductionView(portal));
            category.addItem(new ManageProductionsView(portal));
        }

        // Cluster
        {
            Category category = new Category("Cluster");
            catList.add(category);
            category.addItem(new FrameView(portal, 100, "File System", "http://cvmaster00:50070/dfshealth.jsp"));
            category.addItem(new FrameView(portal, 101, "Job Tracker", "http://cvmaster00:50030/jobtracker.jsp"));
        }

     }

    /**
     * The cell used to render categories.
     */
    private static class CategoryCell extends AbstractCell<Category> {
        @Override
        public void render(Context context, Category value, SafeHtmlBuilder sb) {
            if (value != null) {
                sb.appendEscaped(value.getName());
            }
        }
    }

    /**
     * The cell used to render examples.
     */
    private static class ViewCell extends AbstractCell<PortalView> {
        @Override
        public void render(Context context, PortalView value, SafeHtmlBuilder sb) {
            if (value != null) {
                sb.appendEscaped(value.getTitle());
            }
        }
    }

    /**
     * A top level category in the tree.
     */
    public class Category {

        private final ListDataProvider<PortalView> items = new ListDataProvider<PortalView>();
        private final String name;
        private NodeInfo<PortalView> nodeInfo;

        public Category(String name) {
            this.name = name;
        }

        public void addItem(PortalView item) {
            items.getList().add(item);
            categoryMap.put(item, this);
            viewMap.put(item.getViewId() + "", item);
        }

        public String getName() {
            return name;
        }

        /**
         * Get the node info for the examples under this category.
         *
         * @return the node info
         */
        public NodeInfo<PortalView> getNodeInfo() {
            if (nodeInfo == null) {
                nodeInfo = new DefaultNodeInfo<PortalView>(items, viewCell, selectionModel, null);
            }
            return nodeInfo;
        }
    }
}
