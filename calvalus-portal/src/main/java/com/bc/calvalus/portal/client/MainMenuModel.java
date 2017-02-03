package com.bc.calvalus.portal.client;

import com.google.gwt.cell.client.AbstractCell;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.SelectionModel;
import com.google.gwt.view.client.TreeViewModel;

import java.util.*;

/**
 * The {@link com.google.gwt.view.client.TreeViewModel} used by the main menu.
 */
public class MainMenuModel implements TreeViewModel {

    /**
     * The top level categories.
     */
    private final ListDataProvider<Category> categories = new ListDataProvider<>();

    /**
     * A mapping of {@link PortalView}s to their associated categories.
     */
    private final Map<PortalView, Category> categoryMap = new HashMap<>();

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

    public MainMenuModel(List<PortalView> views, SelectionModel<PortalView> selectionModel) {
        this.selectionModel = selectionModel;
        initialize(views);
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
        Set<PortalView> widgets = new HashSet<>();
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
    private void initialize(List<PortalView> views) {
        List<Category> catList = categories.getList();

        // Order.
        {
            List<PortalView> selectedViews = new ArrayList<>();
            for (PortalView view : views) {
                if (view instanceof OrderProductionView) {
                    selectedViews.add(view);
                }
            }
            if (!selectedViews.isEmpty()) {
                Category category = new Category("Order");
                catList.add(category);
                for (PortalView view : selectedViews) {
                    category.addItem(view);
                }
            }
        }

        // Management.
        {
            List<PortalView> selectedViews = new ArrayList<>();
            for (PortalView view : views) {
                if (view.getViewId().contains(".Manage")) {
                    selectedViews.add(view);
                }
            }
            if (!selectedViews.isEmpty()) {
                Category category = new Category("Management");
                catList.add(category);
                for (PortalView view : selectedViews) {
                    category.addItem(view);
                }
            }
        }

        // Links (FrameView instances)
        {
            List<PortalView> selectedViews = new ArrayList<>();
            for (PortalView view : views) {
                if (view instanceof FrameView) {
                    selectedViews.add(view);
                }
            }
            if (!selectedViews.isEmpty()) {
                Category category = new Category("Links");
                catList.add(category);
                for (PortalView view : selectedViews) {
                    category.addItem(view);
                }
            }
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
                nodeInfo = new DefaultNodeInfo<>(items, viewCell, selectionModel, null);
            }
            return nodeInfo;
        }
    }
}
