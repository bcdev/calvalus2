package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionState;
import com.bc.calvalus.portal.shared.PortalProductionStatus;
import com.google.gwt.cell.client.ButtonCell;
import com.google.gwt.cell.client.Cell;
import com.google.gwt.cell.client.CheckboxCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.safehtml.shared.SafeHtmlUtils;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.cellview.client.SimplePager;
import com.google.gwt.user.cellview.client.TextColumn;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.MultiSelectionModel;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionModel;

import java.util.ArrayList;
import java.util.List;

/**
 * Demo view that shows the list of productions taking place
 * on the production server.
 *
 * @author Norman
 */
public class ManageProductionsView extends PortalView {
    public static final String DOWNLOAD_ACTION_URL = GWT.getModuleBaseURL() + "download";
    public static final int ID = 3;
    private static final String RESTART = "Restart";
    private static final String CANCEL = "Cancel";
    private static final String STAGE = "Stage";
    private static final String DOWNLOAD = "Download";
    private static final String INFO = "Info";

    private FlexTable widget;
    private SelectionModel<PortalProduction> selectionModel;

    public ManageProductionsView(CalvalusPortal portal) {
        super(portal);

        ProvidesKey<PortalProduction> keyProvider = new ProvidesKey<PortalProduction>() {
            public Object getKey(PortalProduction production) {
                return production == null ? null : production.getId();
            }
        };

        selectionModel = new MultiSelectionModel<PortalProduction>(keyProvider);

        CellTable<PortalProduction> productionTable = new CellTable<PortalProduction>(keyProvider);
        productionTable.setWidth("100%");
        productionTable.setSelectionModel(selectionModel);

        Column<PortalProduction, Boolean> checkColumn = new Column<PortalProduction, Boolean>(new CheckboxCell(true, true)) {
            @Override
            public Boolean getValue(PortalProduction production) {
                return selectionModel.isSelected(production);
            }
        };
        checkColumn.setFieldUpdater(new FieldUpdater<PortalProduction, Boolean>() {
            @Override
            public void update(int index, PortalProduction object, Boolean value) {
                selectionModel.setSelected(object, value);
            }
        });

        TextColumn<PortalProduction> nameColumn = new TextColumn<PortalProduction>() {
            @Override
            public String getValue(PortalProduction production) {
                return production.getName();
            }
        };
        nameColumn.setSortable(true);

        TextColumn<PortalProduction> productionStatusColumn = new TextColumn<PortalProduction>() {
            @Override
            public String getValue(PortalProduction production) {
                return getStatusText(production.getProcessingStatus());
            }
        };
        productionStatusColumn.setSortable(true);

        TextColumn<PortalProduction> stagingStatusColumn = new TextColumn<PortalProduction>() {
            @Override
            public String getValue(PortalProduction production) {
                return getStatusText(production.getStagingStatus());
            }
        };
        stagingStatusColumn.setSortable(true);

        Column<PortalProduction, String> actionColumn = new Column<PortalProduction, String>(new ButtonCell()) {
            @Override
            public void render(Cell.Context context, PortalProduction production, SafeHtmlBuilder sb) {
                String action = getAction(production);
                if (action != null) {
                    super.render(context, production, sb);
                } else {
                    sb.appendHtmlConstant("<br/>");
                }
            }

            @Override
            public String getValue(PortalProduction production) {
                return getAction(production);
            }
        };
        actionColumn.setFieldUpdater(new ProductionActionUpdater());

        Column<PortalProduction, String> resultColumn = new Column<PortalProduction, String>(new ButtonCell()) {
            @Override
            public void render(Cell.Context context, PortalProduction production, SafeHtmlBuilder sb) {
                String result = getResult(production);
                if (result != null) {
                    super.render(context, production, sb);
                } else {
                    sb.appendHtmlConstant("<br/>");
                }
            }

            @Override
            public String getValue(PortalProduction production) {
                return getResult(production);
            }
        };
        resultColumn.setFieldUpdater(new ProductionActionUpdater());

        productionTable.addColumn(checkColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        productionTable.addColumn(nameColumn, "Production Name");
        productionTable.addColumn(productionStatusColumn, "Processing Status");
        productionTable.addColumn(stagingStatusColumn, "Staging Status");
        productionTable.addColumn(actionColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        productionTable.addColumn(resultColumn, "Result");

        // Connect the table to the data provider.
        portal.getProductions().addDataDisplay(productionTable);

        // Create a Pager to control the table.
        SimplePager.Resources pagerResources = GWT.create(SimplePager.Resources.class);
        SimplePager pager = new SimplePager(SimplePager.TextLocation.CENTER, pagerResources, false, 0, true);
        pager.setDisplay(productionTable);

        widget = new FlexTable();
        widget.setWidth("100%");
        widget.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_CENTER);
        widget.getFlexCellFormatter().setHorizontalAlignment(1, 0, HasHorizontalAlignment.ALIGN_CENTER);
        widget.getFlexCellFormatter().setHorizontalAlignment(2, 0, HasHorizontalAlignment.ALIGN_LEFT);
        widget.setCellSpacing(4);
        widget.setWidget(0, 0, productionTable);
        widget.setWidget(1, 0, pager);
        widget.setWidget(2, 0, new Button("Delete Selected", new DeleteProductionsAction()));
    }

    static String getResult(PortalProduction production) {
        if (production.getOutputUrl() == null) {
            return null;
        }

        if (production.getProcessingStatus().getState() == PortalProductionState.COMPLETED
                && production.getStagingStatus().getState() == PortalProductionState.COMPLETED) {
            return DOWNLOAD;
        }

        if (production.getProcessingStatus().getState() == PortalProductionState.COMPLETED
                && (production.getStagingStatus().isDone() || production.getStagingStatus().isUnknown())) {
            return STAGE;
        }

        return null;
    }

    static String getAction(PortalProduction production) {
        if (production.getProcessingStatus().isUnknown() && production.getStagingStatus().isUnknown()) {
            return null;
        }
        if (production.getProcessingStatus().isDone()
                && (production.getStagingStatus().isDone() || production.getStagingStatus().isUnknown())) {
            return RESTART;
        } else {
            return CANCEL;
        }
    }


    @Override
    public Widget asWidget() {
        return widget;
    }

    @Override
    public int getViewId() {
        return ID;
    }

    @Override
    public String getTitle() {
        return "Manage Productions";
    }

    /**
     * Starts observing any ongoing productions:.
     */
    @Override
    public void handlePortalStartedUp() {
    }

    private void restartProduction(PortalProduction production) {
        // todo - implement
        Window.alert("Not implemented yet:\n" +
                             "Restart " + production);
    }

    private void showProductionInfo(PortalProduction production) {
        // todo - implement
        Window.alert("Not implemented yet:\n" +
                             "Show info on " + production);
    }

    private void downloadProduction(PortalProduction production) {
/*
        Window.open(DOWNLOAD_ACTION_URL + "?file=" + production.getOutputUrl(),
                    "_blank", "");
*/
        Window.open(production.getOutputUrl(), "_blank", "");
    }

    private void stageProduction(PortalProduction production) {
        getPortal().getBackendService().stageProductions(new String[]{production.getId()}, new AsyncCallback<Void>() {
            @Override
            public void onSuccess(Void ignored) {
                // ok, result will display soon
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Staging failed:\n" + caught.getMessage());
            }
        });
    }

    private void cancelProduction(PortalProduction production) {
        boolean confirm = Window.confirm("Production " + production.getId() + " will be cancelled.\n" +
                                                 "This operation cannot be undone.\n" +
                                                 "\n" +
                                                 "Do you wish to continue?");
        if (!confirm) {
            return;
        }

        getPortal().getBackendService().cancelProductions(new String[]{production.getId()}, new AsyncCallback<Void>() {
            @Override
            public void onSuccess(Void ignored) {
                // ok, result will display soon
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Deletion failed:\n" + caught.getMessage());
            }
        });
    }

    private void deleteProductions(final List<PortalProduction> toDeleteList) {
        if (toDeleteList.isEmpty()) {
            Window.alert("Nothing selected.");
            return;
        }

        boolean confirm = Window.confirm(toDeleteList.size() + " production(s) will be deleted and\n" +
                                                 "associated files will be removed from server.\n" +
                                                 "This operation cannot be undone.\n" +
                                                 "\n" +
                                                 "Do you wish to continue?");
        if (!confirm) {
            return;
        }

        final String[] productionIds = new String[toDeleteList.size()];
        for (int i = 0; i < productionIds.length; i++) {
            productionIds[i] = toDeleteList.get(i).getId();
        }
        getPortal().getBackendService().deleteProductions(productionIds, new AsyncCallback<Void>() {
            @Override
            public void onSuccess(Void ignored) {
                // ok, result will display soon
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Deletion failed:\n" + caught.getMessage());
            }
        });
    }

    private String getStatusText(PortalProductionStatus status) {
        PortalProductionState state = status.getState();
        String message = status.getMessage();
        if (state == PortalProductionState.WAITING) {
            return "Waiting" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == PortalProductionState.IN_PROGRESS) {
            return "In progress (" + (int) (0.5 + status.getProgress() * 100) + "%)" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == PortalProductionState.CANCELLED) {
            return "Cancelled" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == PortalProductionState.ERROR) {
            return "Error" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == PortalProductionState.UNKNOWN) {
            return "Unknown";
        } else if (state == PortalProductionState.COMPLETED) {
            return "Completed" + (message.isEmpty() ? "" : (": " + message));
        }
        return "?";
    }

    private class ProductionActionUpdater implements FieldUpdater<PortalProduction, String> {
        @Override
        public void update(int index, PortalProduction production, String value) {
            if (RESTART.equals(value)) {
                restartProduction(production);
            } else if (CANCEL.equals(value)) {
                cancelProduction(production);
            } else if (DOWNLOAD.equals(value)) {
                downloadProduction(production);
            } else if (STAGE.equals(value)) {
                stageProduction(production);
            } else if (INFO.equals(value)) {
                showProductionInfo(production);
            }
        }

    }

    private class DeleteProductionsAction implements ClickHandler {
        @Override
        public void onClick(ClickEvent event) {
            final List<PortalProduction> availableList = getPortal().getProductions().getList();
            final List<PortalProduction> toDeleteList = new ArrayList<PortalProduction>();
            for (PortalProduction production : availableList) {
                // todo - check, this doesn't work?!?
                if (selectionModel.isSelected(production)) {
                    toDeleteList.add(production);
                }
            }
            deleteProductions(toDeleteList);
        }
    }
}