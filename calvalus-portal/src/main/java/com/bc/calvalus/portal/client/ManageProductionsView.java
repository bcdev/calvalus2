package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.cell.client.ButtonCell;
import com.google.gwt.cell.client.CheckboxCell;
import com.google.gwt.cell.client.FieldUpdater;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.safehtml.shared.SafeHtmlUtils;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.cellview.client.TextColumn;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.CellPreviewEvent;
import com.google.gwt.view.client.DefaultSelectionEventManager;
import com.google.gwt.view.client.ListDataProvider;
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
    public static final int ID = 3;
    private static final int PRODUCTION_UPDATE_PERIOD = 500;
    private static final String RESTART = "Restart";
    private static final String CANCEL = "Cancel";
    private static final String DOWNLOAD = "Download";
    private static final String INFO = "Info";

    private VerticalPanel widget;
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

        // First name.
        TextColumn<PortalProduction> nameColumn = new TextColumn<PortalProduction>() {
            @Override
            public String getValue(PortalProduction production) {
                return production.getName();
            }
        };
        nameColumn.setSortable(true);

        // First name.
        TextColumn<PortalProduction> statusColumn = new TextColumn<PortalProduction>() {
            @Override
            public String getValue(PortalProduction production) {
                WorkStatus status = production.getWorkStatus();
                WorkStatus.State state = status.getState();
                if (state == WorkStatus.State.WAITING) {
                    return "Waiting to start...";
                } else if (state == WorkStatus.State.IN_PROGRESS) {
                    return "In progress (" + (int) (0.5 + status.getProgress() * 100) + "% done)";
                } else if (state == WorkStatus.State.CANCELLED) {
                    return "Cancelled";
                } else if (state == WorkStatus.State.ERROR) {
                    return "Error: " + status.getMessage();
                } else if (state == WorkStatus.State.UNKNOWN) {
                    return "Unknown: " + status.getMessage();
                } else if (state == WorkStatus.State.COMPLETED) {
                    return "Done";
                }
                return "?";
            }
        };
        statusColumn.setSortable(true);

        Column<PortalProduction, String> actionColumn = new Column<PortalProduction, String>(new ButtonCell()) {
            @Override
            public String getValue(PortalProduction production) {
                return production.getWorkStatus().isDone() ? RESTART : CANCEL;
            }
        };
        actionColumn.setFieldUpdater(new ProductionActionUpdater());

        Column<PortalProduction, String> resultColumn = new Column<PortalProduction, String>(new ButtonCell()) {
            @Override
            public String getValue(PortalProduction production) {
                return production.getWorkStatus().getState() == WorkStatus.State.COMPLETED ? DOWNLOAD : INFO;
            }
        };
        resultColumn.setFieldUpdater(new ProductionActionUpdater());

        productionTable.addColumn(checkColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        productionTable.addColumn(nameColumn, "Production Name");
        productionTable.addColumn(statusColumn, "Production Status");
        productionTable.addColumn(actionColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        productionTable.addColumn(resultColumn, "Production Result");

        // Connect the table to the data provider.
        portal.getProductions().addDataDisplay(productionTable);

        widget = new VerticalPanel();
        widget.setSpacing(4);
        widget.add(productionTable);
        widget.add(new Button("Delete Selected", new DeleteProductionsAction()));
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
        ListDataProvider<PortalProduction> productions = getPortal().getProductions();
        List<PortalProduction> productionList = productions.getList();
        for (PortalProduction production : productionList) {
            addProduction(production);
        }
    }

    public void addProduction(PortalProduction production) {
        List<PortalProduction> list = getPortal().getProductions().getList();
        if (!list.contains(production)) {
            list.add(production);
        }

        if (!production.getWorkStatus().isDone()) {
            ProductionHandler productionHandler = new ProductionHandler(production);
            WorkMonitor workMonitor = new WorkMonitor(productionHandler, productionHandler);
            workMonitor.start(PRODUCTION_UPDATE_PERIOD);
        }
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
            } else if (INFO.equals(value)) {
                showProductionInfo(production);
            }
        }
    }

    private void restartProduction(PortalProduction production) {
        // todo - implement
        Window.alert("Not implemented yet:\n" +
                             "Restart " + production);
    }

    private void downloadProduction(PortalProduction production) {
        // todo - implement
        Window.alert("Not implemented yet:\n" +
                             "Download " + production);
    }

    private void showProductionInfo(PortalProduction production) {
        // todo - implement
        Window.alert("Not implemented yet:\n" +
                             "Show info on " + production);
    }

    private void cancelProduction(PortalProduction production) {
        boolean confirm = Window.confirm("Production " + production.getId() + " will be cancelled.\n" +
                                                 "This operation cannot be undone.\n" +
                                                 "\n" +
                                                 "Do you wish to continue?");
        if (!confirm) {
            return;
        }

        getPortal().getBackendService().cancelProductions(new String[]{production.getId()}, new AsyncCallback<boolean[]>() {
            @Override
            public void onSuccess(boolean[] result) {
                // ok
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Cancellation failed:\n" + caught.getMessage());
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
        getPortal().getBackendService().deleteProductions(productionIds, new AsyncCallback<boolean[]>() {
            @Override
            public void onSuccess(boolean[] result) {
                List<PortalProduction> list1 = getPortal().getProductions().getList();
                int deleteCount = 0;
                for (int i = 0; i < result.length; i++) {
                    if (result[i]) {
                        deleteCount++;
                        PortalProduction production = toDeleteList.get(i);
                        list1.remove(production);
                    }
                }
                getPortal().getProductions().refresh();
                Window.alert(deleteCount + " of " + result.length + " production(s) successfully deleted.");
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Deletion failed:\n" + caught.getMessage());
            }
        });
    }


    /**
     * A reporter for production status.
     *
     * @author Norman
     */
    public class ProductionHandler implements WorkReporter, WorkObserver {
        private final PortalProduction production;
        private WorkStatus reportedStatus;

        public ProductionHandler(PortalProduction production) {
            this.production = production;
            this.reportedStatus = production.getWorkStatus();
        }

        @Override
        public WorkStatus getWorkStatus() {
            getPortal().getBackendService().getProductionStatus(production.getId(), new AsyncCallback<WorkStatus>() {
                @Override
                public void onSuccess(WorkStatus result) {
                    reportedStatus = result;
                    production.setWorkStatus(reportedStatus);
                }

                @Override
                public void onFailure(Throwable caught) {
                    reportedStatus = new WorkStatus(WorkStatus.State.UNKNOWN, caught.getMessage(), 0.0);
                    production.setWorkStatus(reportedStatus);
                }
            });
            return reportedStatus;
        }

        @Override
        public void workStarted(WorkStatus status) {
            processWorkStatus(status);
        }

        @Override
        public void workProgressing(WorkStatus status) {
            processWorkStatus(status);
        }

        @Override
        public void workStopped(WorkStatus status) {
            processWorkStatus(status);
        }

        private void processWorkStatus(WorkStatus status) {
            getPortal().getProductions().refresh();
            // GWT.log("processWorkStatus: status=" + status);
        }

        @Override
        public String toString() {
            return "ProductionReporter{" +
                    "production=" + production +
                    ", reportedStatus=" + reportedStatus +
                    '}';
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