package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessState;
import com.bc.calvalus.portal.shared.DtoProcessStatus;
import com.bc.calvalus.portal.shared.DtoProduction;
import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.google.gwt.cell.client.ButtonCell;
import com.google.gwt.cell.client.Cell;
import com.google.gwt.cell.client.CheckboxCell;
import com.google.gwt.cell.client.ClickableTextCell;
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
import com.google.gwt.user.client.ui.HTML;
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
    public static final String ID = ManageProductionsView.class.getName();

    private static final int UPDATE_PERIOD_MILLIS = 2000;

    private static final String RESTART = "Restart";
    private static final String CANCEL = "Cancel";
    private static final String STAGE = "Stage";
    private static final String DOWNLOAD = "Download";
    private static final String INFO = "Info";

    private static final String BEAM_NAME = "BEAM 4.9";
    private static final String BEAM_URL = "http://www.brockmann-consult.de/cms/web/beam/software";
    private static final String BEAM_HTML = "<small>Note: all generated data products may be viewed " +
            "and further processed with <a href=\"" + BEAM_URL + "\" target=\"_blank\">" + BEAM_NAME + "</a></small>";

    private FlexTable widget;
    private SelectionModel<DtoProduction> selectionModel;

    public ManageProductionsView(PortalContext portalContext) {
        super(portalContext);

        ProvidesKey<DtoProduction> keyProvider = new ProvidesKey<DtoProduction>() {
            public Object getKey(DtoProduction production) {
                return production == null ? null : production.getId();
            }
        };

        selectionModel = new MultiSelectionModel<DtoProduction>(keyProvider);

        CellTable<DtoProduction> productionTable = new CellTable<DtoProduction>(keyProvider);
        productionTable.setWidth("100%");
        productionTable.setSelectionModel(selectionModel);

        Column<DtoProduction, Boolean> checkColumn = new Column<DtoProduction, Boolean>(new CheckboxCell(true, true)) {
            @Override
            public Boolean getValue(DtoProduction production) {
                return selectionModel.isSelected(production);
            }
        };
        checkColumn.setFieldUpdater(new FieldUpdater<DtoProduction, Boolean>() {
            @Override
            public void update(int index, DtoProduction object, Boolean value) {
                selectionModel.setSelected(object, value);
            }
        });

        Column<DtoProduction, String> idColumn = new Column<DtoProduction, String>(new ClickableTextCell()) {
            @Override
            public String getValue(DtoProduction production) {
                return production.getId();
            }

        };
        idColumn.setFieldUpdater(new FieldUpdater<DtoProduction, String>() {
            public void update(final int index, final DtoProduction production, final String value) {
                AsyncCallback<DtoProductionRequest> callback = new AsyncCallback<DtoProductionRequest>() {
                    @Override
                    public void onFailure(Throwable caught) {
                    }

                    @Override
                    public void onSuccess(DtoProductionRequest result) {
                        if (result != null) {
                            ShowProductionRequestAction.run(production.getId(), result);
                        } else {
                            Dialog.info(production.getId(), "No production request available.");
                        }
                    }
                };
                getPortal().getBackendService().getProductionRequest(production.getId(), callback);
            }
        });
        idColumn.setSortable(false);

        TextColumn<DtoProduction> nameColumn = new TextColumn<DtoProduction>() {
            @Override
            public String getValue(DtoProduction production) {
                return production.getName();
            }
        };
        nameColumn.setSortable(true);

        TextColumn<DtoProduction> userColumn = new TextColumn<DtoProduction>() {
            @Override
            public String getValue(DtoProduction production) {
                return production.getUser();
            }
        };
        userColumn.setSortable(true);

        TextColumn<DtoProduction> productionStatusColumn = new TextColumn<DtoProduction>() {
            @Override
            public String getValue(DtoProduction production) {
                return getStatusText(production.getProcessingStatus());
            }
        };
        productionStatusColumn.setSortable(true);

        TextColumn<DtoProduction> productionTimeColumn = new TextColumn<DtoProduction>() {
            @Override
            public String getValue(DtoProduction production) {
                return getTimeText(production.getProcessingStatus().getProcessingSeconds());
            }
        };
        productionTimeColumn.setSortable(true);

        TextColumn<DtoProduction> stagingStatusColumn = new TextColumn<DtoProduction>() {
            @Override
            public String getValue(DtoProduction production) {
                return getStatusText(production.getStagingStatus());
            }
        };
        stagingStatusColumn.setSortable(true);

        Column<DtoProduction, String> actionColumn = new Column<DtoProduction, String>(new ButtonCell()) {
            @Override
            public void render(Cell.Context context, DtoProduction production, SafeHtmlBuilder sb) {
                String action = getAction(production);
                if (action != null) {
                    super.render(context, production, sb);
                } else {
                    sb.appendHtmlConstant("<br/>");
                }
            }

            @Override
            public String getValue(DtoProduction production) {
                return getAction(production);
            }
        };
        actionColumn.setFieldUpdater(new ProductionActionUpdater());

        Column<DtoProduction, String> resultColumn = new Column<DtoProduction, String>(new ButtonCell()) {
            @Override
            public void render(Cell.Context context, DtoProduction production, SafeHtmlBuilder sb) {
                String result = getResult(production);
                if (result != null) {
                    if (result.startsWith("#")) {
                        sb.appendHtmlConstant(result.substring(1) + "<br/>");
                    } else {
                        super.render(context, production, sb);
                    }
                } else {
                    sb.appendHtmlConstant("<br/>");
                }
            }

            @Override
            public String getValue(DtoProduction production) {
                return getResult(production);
            }
        };
        resultColumn.setFieldUpdater(new ProductionActionUpdater());

        productionTable.addColumn(checkColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        productionTable.addColumn(idColumn, "Production ID");
        productionTable.addColumn(nameColumn, "Production Name");
        productionTable.addColumn(userColumn, "User");
        productionTable.addColumn(productionStatusColumn, "Processing Status");
        productionTable.addColumn(productionTimeColumn, "Processing Time");
        productionTable.addColumn(stagingStatusColumn, "Staging Status");
        productionTable.addColumn(actionColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        productionTable.addColumn(resultColumn, "Result");

        // Connect the table to the data provider.
        getPortal().getProductions().addDataDisplay(productionTable);

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
        widget.setWidget(3, 0, new HTML(BEAM_HTML));
    }

    static String getResult(DtoProduction production) {
        if (production.getDownloadPath() == null) {
            return null;
        }

        if (production.getProcessingStatus().getState() == DtoProcessState.COMPLETED
                && production.getStagingStatus().getState() == DtoProcessState.UNKNOWN
                && production.isAutoStaging()) {
            return "#Auto-staging";
        }

        if (production.getProcessingStatus().getState() == DtoProcessState.COMPLETED
                && production.getStagingStatus().getState() == DtoProcessState.COMPLETED) {
            return DOWNLOAD;
        }

        if (production.getProcessingStatus().getState() == DtoProcessState.COMPLETED
                && (production.getStagingStatus().isDone() || production.getStagingStatus().isUnknown())) {
            return STAGE;
        }

        return null;
    }

    static String getAction(DtoProduction production) {
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
    public String getViewId() {
        return ID;
    }

    @Override
    public String getTitle() {
        return "Manage Productions";
    }

    @Override
    public void onShowing() {
        getPortal().getProductionsUpdateTimer().scheduleRepeating(UPDATE_PERIOD_MILLIS);
    }

    @Override
    public void onHidden() {
        getPortal().getProductionsUpdateTimer().cancel();
    }

    private void restartProduction(DtoProduction production) {
        // todo - implement
        Window.alert("Not implemented yet:\n" +
                             "Restart " + production);
    }

    private void showProductionInfo(DtoProduction production) {
        // todo - implement
        Window.alert("Not implemented yet:\n" +
                             "Show info on " + production);
    }

    private void downloadProduction(DtoProduction production) {
/*
        Window.open(DOWNLOAD_ACTION_URL + "?file=" + production.getOutputUrl(),
                    "_blank", "");
*/
        Window.open(production.getDownloadPath(), "_blank", "");
    }

    private void stageProduction(DtoProduction production) {
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

    private void cancelProduction(DtoProduction production) {
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

    private void deleteProductions(final List<DtoProduction> toDeleteList) {
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

    private static String getStatusText(DtoProcessStatus status) {
        DtoProcessState state = status.getState();
        String message = status.getMessage();
        if (state == DtoProcessState.UNKNOWN) {
            return "";
        } else if (state == DtoProcessState.SCHEDULED) {
            return "Scheduled" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == DtoProcessState.RUNNING) {
            return "Running (" + (int) (0.5 + status.getProgress() * 100) + "%)" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == DtoProcessState.COMPLETED) {
            return "Completed" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == DtoProcessState.CANCELLED) {
            return "Cancelled" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == DtoProcessState.ERROR) {
            return "Error" + (message.isEmpty() ? "" : (": " + message));
        }
        return "?";
    }

    static String getTimeText(int processingSeconds) {
        if (processingSeconds <= 0) {
            return "";
        } else {
            int hours = processingSeconds / 3600;
            int minutes = processingSeconds / 60 - hours * 60;
            int seconds = processingSeconds - minutes * 60 - hours * 3600;
            return hours + ":" + zeroPadded(minutes) + ":" + zeroPadded(seconds);
        }
    }

    private static String zeroPadded(int value) {
        if (value < 10) {
            return "0" + value;
        } else {
            return "" + value;
        }
    }

    private class ProductionActionUpdater implements FieldUpdater<DtoProduction, String> {
        @Override
        public void update(int index, DtoProduction production, String value) {
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
            final List<DtoProduction> availableList = getPortal().getProductions().getList();
            final List<DtoProduction> toDeleteList = new ArrayList<DtoProduction>();
            for (DtoProduction production : availableList) {
                // todo - check, this doesn't work?!?
                if (selectionModel.isSelected(production)) {
                    toDeleteList.add(production);
                }
            }
            deleteProductions(toDeleteList);
        }
    }
}