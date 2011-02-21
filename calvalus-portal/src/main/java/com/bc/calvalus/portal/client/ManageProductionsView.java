package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.cell.client.ActionCell;
import com.google.gwt.cell.client.CheckboxCell;
import com.google.gwt.safehtml.shared.SafeHtmlUtils;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.cellview.client.IdentityColumn;
import com.google.gwt.user.cellview.client.TextColumn;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.MultiSelectionModel;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionModel;

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

    private CellTable productionTable;

    public ManageProductionsView(CalvalusPortal calvalusPortal) {
        super(calvalusPortal);

        /**
          * The key provider that provides the unique ID of a contact.
          */
        ProvidesKey<PortalProduction> keyProvider = new ProvidesKey<PortalProduction>() {
             public Object getKey(PortalProduction production) {
                 return production == null ? null : production.getId();
             }
         };

        // Set a key provider that provides a unique key for each contact. If key is
        // used to identify contacts when fields (such as the name and address)
        // change.
        this.productionTable = new CellTable<PortalProduction>(keyProvider);
        this.productionTable.setWidth("100%");

        final SelectionModel<PortalProduction> selectionModel = new MultiSelectionModel<PortalProduction>(keyProvider);
        this.productionTable.setSelectionModel(selectionModel);

        Column<PortalProduction, Boolean> checkColumn = new Column<PortalProduction, Boolean>(new CheckboxCell(true, true)) {
            @Override
            public Boolean getValue(PortalProduction production) {
                // Get the value from the selection model.
                return selectionModel.isSelected(production);
            }
        };

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

        ActionCell actionCell = new ActionCell("Info",
                                               new ActionCell.Delegate() {
                                                   @Override
                                                   public void execute(Object object) {
                                                       Window.alert("The file size is bigger than 1 PB and\n" +
                                                                            "downloading it will take approx. 5 years.");
                                                   }
                                               }
        );
        Column<PortalProduction, PortalProduction> resultColumn = new IdentityColumn<PortalProduction>(actionCell);

        this.productionTable.addColumn(checkColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        this.productionTable.addColumn(nameColumn, "Production Name");
        this.productionTable.addColumn(statusColumn, "Production Status");
        this.productionTable.addColumn(resultColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));


        // Connect the table to the data provider.
        calvalusPortal.getProductions().addDataDisplay(this.productionTable);

    }

    @Override
    public Widget asWidget() {
        return productionTable;
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
}