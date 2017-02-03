/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
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

import com.bc.calvalus.portal.shared.*;
import com.google.gwt.cell.client.*;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.logical.shared.ValueChangeEvent;
import com.google.gwt.event.logical.shared.ValueChangeHandler;
import com.google.gwt.safehtml.shared.SafeHtml;
import com.google.gwt.safehtml.shared.SafeHtmlBuilder;
import com.google.gwt.safehtml.shared.SafeHtmlUtils;
import com.google.gwt.user.cellview.client.*;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.RangeChangeEvent;

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

    private static final String SNAP_NAME = "ESA SNAP";
    private static final String SNAP_URL = "http://step.esa.int/main/toolboxes/snap/";
    private static final String GANGLIA_URL = "http://www.brockmann-consult.de/ganglia/";
    private static final String MERCI_URL = "http://calvalus-merci:8080/merci/";
    private static final String SNAP_HTML = "<small>Note: all generated data products may be viewed " +
                                            "and further processed with <a href=\"" + SNAP_URL + "\" target=\"_blank\">" + SNAP_NAME + "</a></small>";
    private static final String GANGLIA_HTML = "<small><a href=\"" + GANGLIA_URL + "\" target=\"_blank\">Ganglia Monitoring</a><br><a href=\"" + MERCI_URL + "\" target=\"_blank\">Calvalus-Catalogue</a></small>";

    static final String RESTART = "Restart";
    static final String EDIT = "Edit";
    static final String CANCEL = "Cancel";
    static final String STAGE = "Stage";
    static final String DOWNLOAD = "Download";

    private FlexTable widget;
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

        selectedProductions = new HashSet<DtoProduction>();

        productionTable = new CellTable<DtoProduction>(keyProvider);
        productionTable.setWidth("100%");

        // Attach a column sort handler to the ListDataProvider to sort the list.
        List<DtoProduction> dtoProductionList = getPortal().getProductions().getList();
        ColumnSortEvent.ListHandler<DtoProduction> sortHandler = new ColumnSortEvent.ListHandler<DtoProduction>(
                dtoProductionList);
        productionTable.addColumnSortHandler(sortHandler);

        CheckboxCell cell = new CheckboxCell(true, true);
        Header<Boolean> checkAllHeader = new Header<Boolean>(cell) {
            @Override
            public Boolean getValue() {
                return selectAll;
            }
        };
        checkAllHeader.setUpdater(new ValueUpdater<Boolean>() {
            @Override
            public void update(Boolean value) {
                if (Boolean.TRUE.equals(value)) {
                    selectAll = true;
                    selectedProductions.addAll(productionTable.getVisibleItems());
                } else {
                    selectAll = false;
                    selectedProductions.clear();
                }
                productionTable.redraw();
            }
        });
        productionTable.addRangeChangeHandler(new RangeChangeEvent.Handler() {
            public void onRangeChange(RangeChangeEvent event) {
                selectAll = false;
                selectedProductions.clear();
            }
        });

        Column<DtoProduction, Boolean> checkColumn = createCheckBoxColumn();
        Column<DtoProduction, String> idColumn = createProductionIDColumn(sortHandler);
        TextColumn<DtoProduction> userColumn = createUserColumn(sortHandler);
        Column<DtoProduction, String> productionStatusColumn = createProductionStatusColum(sortHandler);
        TextColumn<DtoProduction> productionTimeColumn = createProductionTimeColumn(sortHandler);
        TextColumn<DtoProduction> stagingStatusColumn = createStagingStatusColumn(sortHandler);
        Column<DtoProduction, String> actionColumn = createActionColumn();
        Column<DtoProduction, String> stageColumn = createStageColumn();
        Column<DtoProduction, String> downloadColumn = createDownloadColumn();

        productionTable.addColumn(checkColumn, checkAllHeader);
        productionTable.addColumn(idColumn, "Production");
        productionTable.addColumn(userColumn, "User");
        productionTable.addColumn(productionStatusColumn, "Processing Status");
        productionTable.addColumn(productionTimeColumn, "Processing Time");
        productionTable.addColumn(stagingStatusColumn, "Staging Status");
        productionTable.addColumn(actionColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        productionTable.addColumn(stageColumn, "Result");
        productionTable.addColumn(downloadColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));

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
        allUsers.setEnabled(getPortal().withPortalFeature("otherSets"));
        allUsers.addValueChangeHandler(new ValueChangeHandler<Boolean>() {
            @Override
            public void onValueChange(ValueChangeEvent<Boolean> booleanValueChangeEvent) {
                getPortal().setProductionListFiltered(!allUsers.getValue());
            }
        });

        Anchor manageProductionsHelp = new Anchor("Show Help");
        HelpSystem.addClickHandler(manageProductionsHelp, "manageProductions");


        widget = new FlexTable();
        widget.setWidth("100%");
        FlexTable.FlexCellFormatter flexCellFormatter = widget.getFlexCellFormatter();
        flexCellFormatter.setHorizontalAlignment(0, 0, HasHorizontalAlignment.ALIGN_RIGHT);
        flexCellFormatter.setColSpan(0, 0, 2);
        flexCellFormatter.setHorizontalAlignment(1, 0, HasHorizontalAlignment.ALIGN_CENTER);
        flexCellFormatter.setColSpan(1, 0, 2);
        flexCellFormatter.setHorizontalAlignment(2, 0, HasHorizontalAlignment.ALIGN_CENTER);
        flexCellFormatter.setColSpan(2, 0, 2);
        flexCellFormatter.setHorizontalAlignment(3, 0, HasHorizontalAlignment.ALIGN_LEFT);
        flexCellFormatter.setHorizontalAlignment(3, 1, HasHorizontalAlignment.ALIGN_RIGHT);
        flexCellFormatter.setHorizontalAlignment(4, 0, HasHorizontalAlignment.ALIGN_LEFT);
        flexCellFormatter.setHorizontalAlignment(4, 1, HasHorizontalAlignment.ALIGN_RIGHT);
        widget.setCellSpacing(4);
        widget.setWidget(0, 0, allUsers);
        widget.setWidget(1, 0, productionTable);
        widget.setWidget(2, 0, pager);
        widget.setWidget(3, 0, new Button("Delete Selected", new DeleteProductionsAction()));
        widget.setWidget(3, 1, manageProductionsHelp);
        widget.setWidget(4, 0, new HTML(SNAP_HTML));
        if (portalContext.withPortalFeature("catalogue")) {
            widget.setWidget(4, 1, new HTML(GANGLIA_HTML));
        }

        fireSortListEvent();
    }

    private Column<DtoProduction, String> createStageColumn() {
        Column<DtoProduction, String> resultColumn = new Column<DtoProduction, String>(new ButtonCell()) {
            @Override
            public void render(Cell.Context context, DtoProduction production, SafeHtmlBuilder sb) {
                StageType stageType = getStageType(production);
                switch (stageType) {
                    case NO_STAGING:
                        sb.appendHtmlConstant("<br/>");
                        break;
                    case AUTO_STAGING:
                        sb.appendHtmlConstant(stageType.getText() + "<br/>");
                        break;
                    case STAGE:
                    case MULTI_STAGE:
                        super.render(context, production, sb);
                        break;
                    default:
                        throw new IllegalStateException("Unknown StageType");
                }
            }

            @Override
            public String getValue(DtoProduction production) {
                return getStageType(production).getText();
            }
        };
        resultColumn.setFieldUpdater(new ProductionActionUpdater());
        return resultColumn;
    }

    private Column<DtoProduction, String> createDownloadColumn() {
        Column<DtoProduction, String> resultColumn = new Column<DtoProduction, String>(new ButtonCell()) {
            @Override
            public void render(Cell.Context context, DtoProduction production, SafeHtmlBuilder sb) {
                String result = getDownloadText(production);
                if (result != null) {
                    if (result.startsWith("#")) { // means auto staging
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
                return getDownloadText(production);
            }
        };
        resultColumn.setFieldUpdater(new ProductionActionUpdater());
        return resultColumn;
    }

    private Column<DtoProduction, String> createActionColumn() {
        Column<DtoProduction, String> actionColumn = new Column<DtoProduction, String>(new ButtonCell()) {
            @Override
            public void render(Cell.Context context, DtoProduction production, SafeHtmlBuilder sb) {
                String action = getAction(production, isRestorable(production));
                if (action != null) {
                    super.render(context, production, sb);
                } else {
                    sb.appendHtmlConstant("<br/>");
                }
            }

            @Override
            public String getValue(DtoProduction production) {
                return getAction(production, isRestorable(production));
            }

            private boolean isRestorable(DtoProduction production) {
                return getPortal().getViewForRestore(production.getProductionType()) != null;
            }
        };
        actionColumn.setFieldUpdater(new ProductionActionUpdater());
        return actionColumn;
    }

    private TextColumn<DtoProduction> createStagingStatusColumn(
            ColumnSortEvent.ListHandler<DtoProduction> sortHandler) {
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
        return stagingStatusColumn;
    }

    private TextColumn<DtoProduction> createProductionTimeColumn(
            ColumnSortEvent.ListHandler<DtoProduction> sortHandler) {
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
        return productionTimeColumn;
    }

    private Column<DtoProduction, String> createProductionStatusColum(ColumnSortEvent.ListHandler<DtoProduction> sortHandler) {

        ClickableTextCell clickableTextCell = new ClickableTextCell();
        Column<DtoProduction, String> productionStatusColumn = new Column<DtoProduction, String>(clickableTextCell) {
            @Override
            public void render(Cell.Context context, DtoProduction production, SafeHtmlBuilder sb) {
                DtoProcessStatus status = production.getProcessingStatus();
                DtoProcessState state = status.getState();

                if (state == DtoProcessState.COMPLETED || state == DtoProcessState.ERROR) {
                    String url = GWT.getModuleBaseURL() + "hadoopLogs?productionId=" + production.getId();
                    sb.appendHtmlConstant("<a href=\"" + url + "\" target=\"_blank\">" + state.toString() + "</a>");
                } else if (state == DtoProcessState.RUNNING) {
                    sb.appendEscaped(state.toString() + " (" + getProgressText(status.getProgress()) + ")");
                } else {
                    sb.appendEscaped(state.toString());
                }
                if (!status.getMessage().isEmpty()) {
                    // TODO (mz 2013-11-19) replace by Tooltip to save screen real estate
                    sb.appendHtmlConstant("<br/>");
                    sb.appendHtmlConstant("<font size=\"-2\" color=\"#777799\">");
                    sb.appendEscaped(status.getMessage());
                    sb.appendHtmlConstant("</font>");
                }
            }

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
        return productionStatusColumn;
    }

    private TextColumn<DtoProduction> createUserColumn(ColumnSortEvent.ListHandler<DtoProduction> sortHandler) {
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
        return userColumn;
    }

    private Column<DtoProduction, String> createProductionIDColumn(
            ColumnSortEvent.ListHandler<DtoProduction> sortHandler) {
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
        return idColumn;
    }

    private Column<DtoProduction, Boolean> createCheckBoxColumn() {
        Column<DtoProduction, Boolean> checkColumn = new Column<DtoProduction, Boolean>(
                new CheckboxCell(false, false)) {
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
        return checkColumn;
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

    @Override
    public PortalContext getPortal() {
        return super.getPortal();
    }

    static StageType getStageType(DtoProduction production) {
        if (production.isAutoStaging()) {
            if (isProcessingCompleted(production)) {
                if (isStagingRunning(production) ||
                    isStagingScheduled(production)) {
                    return StageType.NO_STAGING;
                } else if (isNotSuccessfulStaged(production)) {
                    return StageType.STAGE;
                } else {
                    return StageType.AUTO_STAGING;
                }
            } else {
                return StageType.NO_STAGING;
            }
        }

        if (isNotSuccessfulStaged(production) || isUnknownStagingState(production)) {
            if (isProcessingCompleted(production)) {
                if (hasAdditionalStagingPaths(production)) {
                    return StageType.MULTI_STAGE;
                } else {
                    return StageType.STAGE;
                }
            }
        }

        if (isStagingScheduled(production) ||
            isStagingRunning(production) ||
            isStagingCompleted(production)) {
            if (hasAdditionalStagingPaths(production)) {
                return StageType.MULTI_STAGE;
            }
        }

        return StageType.NO_STAGING;
    }

    private static boolean isUnknownStagingState(DtoProduction production) {
        return production.getStagingStatus().getState() == DtoProcessState.UNKNOWN;
    }

    private static boolean isStagingCompleted(DtoProduction production) {
        return production.getStagingStatus().getState() == DtoProcessState.COMPLETED;
    }

    private static boolean isStagingRunning(DtoProduction production) {
        return production.getStagingStatus().getState() == DtoProcessState.RUNNING;
    }

    private static boolean isStagingScheduled(DtoProduction production) {
        return production.getStagingStatus().getState() == DtoProcessState.SCHEDULED;
    }

    private static boolean isProcessingCompleted(DtoProduction production) {
        return production.getProcessingStatus().getState() == DtoProcessState.COMPLETED;
    }


    private static boolean isNotSuccessfulStaged(DtoProduction production) {
        return production.getStagingStatus().getState() == DtoProcessState.CANCELLED
               || production.getStagingStatus().getState() == DtoProcessState.ERROR;
    }

    private static boolean hasAdditionalStagingPaths(DtoProduction production) {
        return production.getAdditionalStagingPaths() != null && production.getAdditionalStagingPaths().length > 0;
    }

    static String getDownloadText(DtoProduction production) {
        if (production.getDownloadPath() != null
            && production.getProcessingStatus().getState() == DtoProcessState.COMPLETED
            && production.getStagingStatus().getState() == DtoProcessState.COMPLETED) {
            return DOWNLOAD;
        }

        return null;
    }


    static String getAction(DtoProduction production, boolean isRestorable) {
        if (production.getProcessingStatus().isUnknown() && production.getStagingStatus().isUnknown()) {
            return null;
        }
        if (production.getProcessingStatus().isDone()
            && (production.getStagingStatus().isDone() || production.getStagingStatus().isUnknown())) {
            if (isRestorable) {
                return EDIT;
            } else {
                return RESTART;
            }
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
        return "Productions";
    }

    @Override
    public void onShowing() {
        getPortal().getProductionsUpdateTimer().scheduleRepeating(UPDATE_PERIOD_MILLIS);
    }

    @Override
    public void onHidden() {
        getPortal().getProductionsUpdateTimer().cancel();
    }

    private void restartProduction(final DtoProduction production) {
        final BackendServiceAsync backendService = getPortal().getBackendService();

        AsyncCallback<DtoProductionRequest> callback = new AsyncCallback<DtoProductionRequest>() {
            @Override
            public void onFailure(Throwable caught) {
                Dialog.info(production.getId(), "No production request available.");
            }

            @Override
            public void onSuccess(final DtoProductionRequest request) {
                if (request != null) {
                    final DialogBox submitDialog = OrderProductionView.createSubmitProductionDialog();
                    submitDialog.center();

                    backendService.orderProduction(request, new AsyncCallback<DtoProductionResponse>() {
                        public void onSuccess(final DtoProductionResponse response) {
                            submitDialog.hide();
                        }

                        public void onFailure(Throwable failure) {
                            submitDialog.hide();
                            failure.printStackTrace(System.err);
                            Dialog.error("Server-side Production Error", failure.getMessage());
                        }
                    });

                } else {
                    Dialog.info(production.getId(), "No production request available.");
                }
            }
        };
        backendService.getProductionRequest(production.getId(), callback);
    }

    private void editProduction(final DtoProduction production) {
        final BackendServiceAsync backendService = getPortal().getBackendService();

        AsyncCallback<DtoProductionRequest> callback = new AsyncCallback<DtoProductionRequest>() {
            @Override
            public void onFailure(Throwable caught) {
                Dialog.info(production.getId(), "No production request available.");
            }

            @Override
            public void onSuccess(final DtoProductionRequest request) {
                if (request != null) {
                    String productionType = request.getProductionType();
                    OrderProductionView orderProductionView = getPortal().getViewForRestore(productionType);
                    orderProductionView.setProductionParameters(request.getProductionParameters());
                    getPortal().showView(orderProductionView.getViewId());
                } else {
                    Dialog.info(production.getId(), "No production request available.");
                }
            }
        };
        backendService.getProductionRequest(production.getId(), callback);
    }

    private void downloadProduction(DtoProduction production) {
        Window.open(GWT.getHostPageBaseURL() + production.getDownloadPath(), "_blank", "");
    }

    private void stageProduction(DtoProduction production, String scpPath) {
        BackendServiceAsync backendService = getPortal().getBackendService();
        backendService.scpProduction(production.getId(), scpPath, new AsyncCallback<Void>() {
            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Server-side Staging Error", caught.getMessage());
            }

            @Override
            public void onSuccess(Void result) {
                // ok, result will be at destination
            }
        });
    }

    private void stageProduction(DtoProduction production) {
        AsyncCallback<Void> callback = new AsyncCallback<Void>() {
            @Override
            public void onSuccess(Void ignored) {
                // ok, result will display soon
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Server-side Staging Error", caught.getMessage());
            }
        };
        getPortal().getBackendService().stageProductions(new String[]{production.getId()}, callback);
    }

    private void cancelProduction(DtoProduction production) {
        boolean confirm = Window.confirm("Production " + production.getId() + " will be cancelled.\n" +
                                         "This operation cannot be undone.\n" +
                                         "\n" +
                                         "Do you wish to continue?");
        if (!confirm) {
            return;
        }

        AsyncCallback<Void> callback = new AsyncCallback<Void>() {
            @Override
            public void onSuccess(Void ignored) {
                // ok, result will display soon
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Server-side Cancellation Error", caught.getMessage());
            }
        };
        getPortal().getBackendService().cancelProductions(new String[]{production.getId()}, callback);
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
        AsyncCallback<Void> callback = new AsyncCallback<Void>() {
            @Override
            public void onSuccess(Void ignored) {
                // ok, result will display soon
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Server-side Deletion Error", caught.getMessage());
            }
        };
        getPortal().getBackendService().deleteProductions(productionIds, callback);
    }

    private static String getStatusText(DtoProcessStatus status) {
        DtoProcessState state = status.getState();
        String message = status.getMessage();
        if (state == DtoProcessState.UNKNOWN) {
            return "UNKNOWN";
        } else if (state == DtoProcessState.SCHEDULED) {
            return "SCHEDULED" + (message.isEmpty() ? "" : (": " + message));
        } else if (state == DtoProcessState.RUNNING) {
            return "RUNNING (" + getProgressText(
                    status.getProgress()) + ")" + (message.isEmpty() ? "" : (": " + message));
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
            } else if (EDIT.equals(value)) {
                editProduction(production);
            } else if (CANCEL.equals(value)) {
                cancelProduction(production);
            } else if (DOWNLOAD.equals(value)) {
                downloadProduction(production);
            } else if (STAGE.equals(value)) {
                String[] stagingPaths = production.getAdditionalStagingPaths();
                if (stagingPaths.length > 0) {
                    showStageDialog(production);
                } else {
                    stageProduction(production);
                }

            }
        }

    }

    private void showStageDialog(final DtoProduction production) {
        String[] stagingPaths = production.getAdditionalStagingPaths();
        final Map<RadioButton, String> buttonMap = new LinkedHashMap<RadioButton, String>();
        RadioButton stageButton = new RadioButton("selection", "Default Staging");
        boolean isDownloadable = getDownloadText(production) != null;
        stageButton.setValue(!isDownloadable);
        stageButton.setEnabled(!isDownloadable);
        buttonMap.put(stageButton, "");
        for (int i = 0; i < stagingPaths.length; i++) {
            String stagingPath = stagingPaths[i].trim();
            SafeHtml label = SafeHtmlUtils.fromSafeConstant("Stage to <i>" + stagingPath + "</i>");
            RadioButton additionalStageButton = new RadioButton("selection", label);
            if (i == 0) {
                additionalStageButton.setValue(isDownloadable);
            }
            additionalStageButton.setEnabled(isDownloadable);
            buttonMap.put(additionalStageButton, stagingPath);
        }

        VerticalPanel dialogContent = new VerticalPanel();
        SafeHtmlBuilder htmlBuilder = new SafeHtmlBuilder();
        htmlBuilder.appendHtmlConstant("<b>Be careful!</b></br>");
        htmlBuilder.appendHtmlConstant("If you activate another staging than the default it is most likely that the " +
                                       "result is immediately published and publicly available.</br>");
        htmlBuilder.appendHtmlConstant("<hr>");
        htmlBuilder.appendHtmlConstant("</br>");
        dialogContent.add(new HTML(htmlBuilder.toSafeHtml()));
        for (RadioButton radioButton : buttonMap.keySet()) {
            dialogContent.add(radioButton);
        }
        ScrollPanel scrollPanel = new ScrollPanel(dialogContent);
        scrollPanel.setWidth("500px");
        scrollPanel.setHeight("200px");

        Dialog dialog = new Dialog("Select Staging", scrollPanel, Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
            @Override
            protected void onHide() {
                if (ButtonType.OK == getSelectedButtonType()) {
                    for (RadioButton radioButton : buttonMap.keySet()) {
                        if (radioButton.getValue()) {
                            String stagePath = buttonMap.get(radioButton);
                            if (!stagePath.isEmpty()) {
                                stageProduction(production, stagePath);
                            } else {
                                stageProduction(production);
                            }
                        }
                    }
                }
            }
        };

        dialog.show();
    }

    private class DeleteProductionsAction implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            deleteProductions(new ArrayList<DtoProduction>(selectedProductions));
            selectedProductions.clear();
        }
    }
}
