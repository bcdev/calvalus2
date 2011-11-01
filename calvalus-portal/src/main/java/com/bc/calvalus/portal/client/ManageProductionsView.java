package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessState;
import com.bc.calvalus.portal.shared.DtoProcessStatus;
import com.bc.calvalus.portal.shared.DtoProduction;
import com.bc.calvalus.portal.shared.DtoProductionRequest;
import com.google.gwt.cell.client.*;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.safehtml.shared.SafeHtmlUtils;
import com.google.gwt.user.cellview.client.*;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionModel;

import java.util.*;

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

    private static final String BEAM_NAME = "BEAM 4.9";
    private static final String BEAM_URL = "http://www.brockmann-consult.de/cms/web/beam/software";
    private static final String BEAM_HTML = "<small>Note: all generated data products may be viewed " +
            "and further processed with <a href=\"" + BEAM_URL + "\" target=\"_blank\">" + BEAM_NAME + "</a></small>";

    private FlexTable widget;
    private SelectionModel<DtoProduction> selectionModel;
    private CellTable<DtoProduction> productionTable;
    private boolean selectAll;
    private Set<DtoProduction> selectedProductions;

    public ManageProductionsView(PortalContext portalContext) {
        super(portalContext);

        ProvidesKey<DtoProduction> keyProvider = new ProvidesKey<DtoProduction>() {
            public Object getKey(DtoProduction production) {
                return production == null ? null : production.getId();
            }
        };

        // selectionModel = new MultiSelectionModel<DtoProduction>(keyProvider);
        selectedProductions = new HashSet<DtoProduction>();

        productionTable = new CellTable<DtoProduction>(keyProvider);
        productionTable.setWidth("100%");
        //productionTable.setSelectionModel(selectionModel);

        // Attach a column sort handler to the ListDataProvider to sort the list.
        List<DtoProduction> dtoProductionList = getPortal().getProductions().getList();
        ColumnSortEvent.ListHandler<DtoProduction> sortHandler = new ColumnSortEvent.ListHandler<DtoProduction>(dtoProductionList);
        productionTable.addColumnSortHandler(sortHandler);

        CheckboxCell cell = new CheckboxCell(true, true);
        Header<Boolean> checkAllHeader = new Header<Boolean>(cell) {
            @Override
            public Boolean getValue() {
                GWT.log("checkAllHeader.getValue() = " + selectAll);
                return selectAll;
            }
        };
        checkAllHeader.setUpdater(new ValueUpdater<Boolean>() {
            @Override
            public void update(Boolean value) {
                GWT.log("checkAllHeader.update(" + value + ")");
                final List<DtoProduction> productions = getPortal().getProductions().getList();
                if (Boolean.TRUE.equals(value)) {
                    selectAll = true;
                    selectedProductions.addAll(productions);
                } else {
                    selectAll = false;
                    selectedProductions.clear();
                }
                productionTable.redraw();
            }
        });

        Column<DtoProduction, Boolean> checkColumn = new Column<DtoProduction, Boolean>(new CheckboxCell(false, false)) {
            @Override
            public Boolean getValue(DtoProduction production) {
                return selectedProductions.contains(production);
            }
        };
        checkColumn.setFieldUpdater(new FieldUpdater<DtoProduction, Boolean>() {
            @Override
            public void update(int index, DtoProduction production, Boolean value) {
                if (Boolean.TRUE.equals(value)) {
                    selectedProductions.add(production);
                } else {
                    selectedProductions.remove(production);
                }
            }
        });

        Column<DtoProduction, String> idColumn = new Column<DtoProduction, String>(new ClickableTextCell()) {
            @Override
            public void render(Cell.Context context, DtoProduction production, SafeHtmlBuilder sb) {
                String productionId = production.getId();
                String productionName = production.getName();

                sb.appendHtmlConstant("<font size=\"-2\" color=\"#777799\">");
                sb.appendEscaped(productionId);
                sb.appendHtmlConstant("</font>");
                sb.appendHtmlConstant("<br/>");
                sb.appendEscaped(productionName);

                String inventoryPath = production.getInventoryPath();
                if (inventoryPath != null) {
                    String searchString = "/calvalus/";
                    int pos = inventoryPath.indexOf(searchString);
                    if (pos > 0) {
                        inventoryPath = inventoryPath.substring(pos + searchString.length());
                    }
                    sb.appendHtmlConstant("<br/>");
                    sb.appendHtmlConstant("<font size=\"-2\" color=\"#779977\">");
                    sb.appendEscaped(inventoryPath);
                    sb.appendHtmlConstant("</font>");
                }
            }

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
        idColumn.setSortable(true);
        sortHandler.setComparator(idColumn, new Comparator<DtoProduction>() {
            public int compare(DtoProduction p1, DtoProduction p2) {
                return p1.getId().compareTo(p2.getId());
            }
        });

        TextColumn<DtoProduction> userColumn = new TextColumn<DtoProduction>() {
            @Override
            public String getValue(DtoProduction production) {
                return production.getUser();
            }
        };
        userColumn.setSortable(true);
        sortHandler.setComparator(userColumn, new Comparator<DtoProduction>() {
            public int compare(DtoProduction p1, DtoProduction p2) {
                return p1.getUser().compareTo(p2.getUser());
            }
        });

        TextColumn<DtoProduction> productionStatusColumn = new TextColumn<DtoProduction>() {
            @Override
            public String getValue(DtoProduction production) {
                return getStatusText(production.getProcessingStatus());
            }
        };
        productionStatusColumn.setSortable(true);
        sortHandler.setComparator(productionStatusColumn, new Comparator<DtoProduction>() {
            public int compare(DtoProduction p1, DtoProduction p2) {
                return p1.getProcessingStatus().getState().compareTo(p2.getProcessingStatus().getState());
            }
        });

        TextColumn<DtoProduction> productionTimeColumn = new TextColumn<DtoProduction>() {
            @Override
            public String getValue(DtoProduction production) {
                return getTimeText(production.getProcessingStatus().getProcessingSeconds());
            }
        };
        productionTimeColumn.setSortable(true);
        sortHandler.setComparator(productionTimeColumn, new Comparator<DtoProduction>() {
            public int compare(DtoProduction p1, DtoProduction p2) {
                Integer p1Sec = p1.getProcessingStatus().getProcessingSeconds();
                Integer p2Sec = p2.getProcessingStatus().getProcessingSeconds();
                return p1Sec.compareTo(p2Sec);
            }
        });

        TextColumn<DtoProduction> stagingStatusColumn = new TextColumn<DtoProduction>() {
            @Override
            public String getValue(DtoProduction production) {
                return getStatusText(production.getStagingStatus());
            }
        };
        stagingStatusColumn.setSortable(true);
        sortHandler.setComparator(stagingStatusColumn, new Comparator<DtoProduction>() {
            public int compare(DtoProduction p1, DtoProduction p2) {
                return p1.getStagingStatus().getState().compareTo(p2.getStagingStatus().getState());
            }
        });

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

        productionTable.addColumn(checkColumn, checkAllHeader);
        productionTable.addColumn(idColumn, "Production");
        productionTable.addColumn(userColumn, "User");
        productionTable.addColumn(productionStatusColumn, "Processing Status");
        productionTable.addColumn(productionTimeColumn, "Processing Time");
        productionTable.addColumn(stagingStatusColumn, "Staging Status");
        productionTable.addColumn(actionColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        productionTable.addColumn(resultColumn, "Result");

        // Connect the table to the data provider.
        getPortal().getProductions().addDataDisplay(productionTable);
        ColumnSortList.ColumnSortInfo sortInfo = new ColumnSortList.ColumnSortInfo(idColumn, false);
        productionTable.getColumnSortList().push(sortInfo);

        // Create a Pager to control the table.
        SimplePager.Resources pagerResources = GWT.create(SimplePager.Resources.class);
        SimplePager pager = new SimplePager(SimplePager.TextLocation.CENTER, pagerResources, false, 0, true);
        pager.setDisplay(productionTable);

        final CheckBox allUsers = new CheckBox("Show productions of all users");
        allUsers.setValue(!getPortal().isProductionListFiltered());
        allUsers.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
                getPortal().setProductionListFiltered(!allUsers.getValue());
            }
        });

        widget = new FlexTable();
        widget.setWidth("100%");
        widget.getFlexCellFormatter().setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_RIGHT);
        widget.getFlexCellFormatter().setHorizontalAlignment(1, 0, HasHorizontalAlignment.ALIGN_CENTER);
        widget.getFlexCellFormatter().setHorizontalAlignment(2, 0, HasHorizontalAlignment.ALIGN_CENTER);
        widget.getFlexCellFormatter().setHorizontalAlignment(3, 0, HasHorizontalAlignment.ALIGN_LEFT);
        widget.getFlexCellFormatter().setHorizontalAlignment(4, 0, HasHorizontalAlignment.ALIGN_LEFT);
        widget.setCellSpacing(4);
        widget.setWidget(0, 0, allUsers);
        widget.setWidget(1, 0, productionTable);
        widget.setWidget(2, 0, pager);
        widget.setWidget(3, 0, new Button("Delete Selected", new DeleteProductionsAction()));
        widget.setWidget(4, 0, new HTML(BEAM_HTML));

        fireSortListEvent();
    }

    public static String getProgressText(float progress) {
        int integer = (int) Math.floor(progress * 100);
        int fract = (int) Math.floor(10 * (progress * 100 - integer));
        return integer + "." + fract + "%";
    }

    void fireSortListEvent() {
        if (productionTable != null) {
            ColumnSortEvent.fire(productionTable, productionTable.getColumnSortList());
        }
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
        // todo - implement 'Restart'
        Dialog.error("Not Implemented", "Sorry, 'Restart' has not been implemented yet.");
    }

    private void downloadProduction(DtoProduction production) {
        Window.open(GWT.getHostPageBaseURL() + production.getDownloadPath(), "_blank", "");
    }

    private void stageProduction(DtoProduction production) {
        getPortal().getBackendService().stageProductions(new String[]{production.getId()}, new AsyncCallback<Void>() {
            @Override
            public void onSuccess(Void ignored) {
                // ok, result will display soon
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Server-side Staging Error", caught.getMessage());
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
                Dialog.error("Server-side Cancellation Error", caught.getMessage());
            }
        });
    }

    private void deleteProductions(final List<DtoProduction> toDeleteList) {
        if (toDeleteList.isEmpty()) {
            Dialog.error("Warning", "No production selected.<br/>Please select one or more productions first.");
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
                Dialog.error("Server-side Deletion Error", caught.getMessage());
            }
        });
    }

    private static String getStatusText(DtoProcessStatus status) {
        DtoProcessState state = status.getState();
        String message = status.getMessage();
        if (state == DtoProcessState.UNKNOWN) {
            return "UNKNOWN";
        } else if (state == DtoProcessState.SCHEDULED) {
            return "SCHEDULED" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == DtoProcessState.RUNNING) {
            return "RUNNING (" + getProgressText(status.getProgress()) + ")" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == DtoProcessState.COMPLETED) {
            return "COMPLETED" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == DtoProcessState.CANCELLED) {
            return "CANCELLED" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == DtoProcessState.ERROR) {
            return "ERROR" + (message.isEmpty() ? "" : (": " + message));
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
            }
        }

    }

    private class DeleteProductionsAction implements ClickHandler {
        @Override
        public void onClick(ClickEvent event) {
            deleteProductions(new ArrayList<DtoProduction>(selectedProductions));
            selectedProductions.clear();
        }
    }
}
