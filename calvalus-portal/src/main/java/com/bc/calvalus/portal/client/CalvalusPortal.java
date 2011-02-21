package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.cell.client.ActionCell;
import com.google.gwt.cell.client.CheckboxCell;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.safehtml.shared.SafeHtmlUtils;
import com.google.gwt.user.cellview.client.CellTable;
import com.google.gwt.user.cellview.client.Column;
import com.google.gwt.user.cellview.client.IdentityColumn;
import com.google.gwt.user.cellview.client.TextColumn;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DecoratedTabPanel;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FlexTable;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HasHorizontalAlignment;
import com.google.gwt.user.client.ui.HasVerticalAlignment;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;
import com.google.gwt.view.client.ListDataProvider;
import com.google.gwt.view.client.MultiSelectionModel;
import com.google.gwt.view.client.ProvidesKey;
import com.google.gwt.view.client.SelectionModel;
import org.gwtopenmaps.openlayers.client.LonLat;
import org.gwtopenmaps.openlayers.client.MapOptions;
import org.gwtopenmaps.openlayers.client.MapWidget;
import org.gwtopenmaps.openlayers.client.control.LayerSwitcher;
import org.gwtopenmaps.openlayers.client.control.MousePosition;
import org.gwtopenmaps.openlayers.client.control.NavToolbar;
import org.gwtopenmaps.openlayers.client.control.PanZoomBar;
import org.gwtopenmaps.openlayers.client.layer.Layer;
import org.gwtopenmaps.openlayers.client.layer.TransitionEffect;
import org.gwtopenmaps.openlayers.client.layer.WMS;
import org.gwtopenmaps.openlayers.client.layer.WMSOptions;
import org.gwtopenmaps.openlayers.client.layer.WMSParams;

import java.util.Arrays;
import java.util.List;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 *
 * @author Norman
 */
public class CalvalusPortal implements EntryPoint {

    static final String UPLOAD_ACTION_URL = GWT.getModuleBaseURL() + "upload";

    private final BackendServiceAsync backendService = GWT.create(BackendService.class);

    private FormPanel form;
    private ListBox processorListBox;
    private ListBox processorVersionListBox;
    private ListBox inputPsListBox;
    private TextArea parametersTextArea;
    private FileUpload parametersFileUpload;
    private TextBox outputPsTextBox;

    private boolean initialised;

    private DecoratedTabPanel tabPanel;
    private PortalProductSet[] productSets;
    private PortalProcessor[] processors;
    private ListDataProvider<PortalProduction> productions;

    /**
     * This is the entry point method.
     */
    @Override
    public void onModuleLoad() {
         productions = new ListDataProvider<PortalProduction>();
        backendService.getProductSets(null, new AsyncCallback<PortalProductSet[]>() {
            @Override
            public void onSuccess(PortalProductSet[] productSets) {
                CalvalusPortal.this.productSets = productSets;
                maybeInitFrontend();
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Error!\n" + caught.getMessage());
                CalvalusPortal.this.productSets = new PortalProductSet[0];
            }
        });

        backendService.getProcessors(null, new AsyncCallback<PortalProcessor[]>() {
            @Override
            public void onSuccess(PortalProcessor[] processors) {
                CalvalusPortal.this.processors = processors;
                maybeInitFrontend();
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Error!\n" + caught.getMessage());
                CalvalusPortal.this.processors = new PortalProcessor[0];
            }
        });

        backendService.getProductions(null, new AsyncCallback<PortalProduction[]>() {
            @Override
            public void onSuccess(PortalProduction[] productions) {
                CalvalusPortal.this.productions.getList().addAll(Arrays.asList(productions));
                maybeInitFrontend();
            }

            @Override
            public void onFailure(Throwable caught) {
               Window.alert("Error!\n" + caught.getMessage());
            }
        });
    }

    private void maybeInitFrontend() {
        if (!initialised && isAllInputDataAvailable()) {
            initialised = true;
            initFrontend();
        }
    }

    private void initFrontend() {
        tabPanel = new DecoratedTabPanel();
        tabPanel.setWidth("600px");
        tabPanel.setAnimationEnabled(true);

        tabPanel.add(createQueryPanel(), "Create Product Sets");
        tabPanel.add(createLevel2Panel(), "Level 2 Processor");
        tabPanel.add(createJobsPanel(), "Productions");

        tabPanel.selectTab(1);
        tabPanel.ensureDebugId("cwTabPanel");

        DOM.removeChild(RootPanel.getBodyElement(), DOM.getElementById("splashScreen"));
        RootPanel.get("mainPanel").add(tabPanel);

        // Start observing any ongoing productions:
        List<PortalProduction> productionList = productions.getList();
        for (PortalProduction production : productionList) {
            if (!production.getWorkStatus().isDone()) {
                WorkMonitor workMonitor = new WorkMonitor(new ProductionReporter(backendService, production),
                                           new ProductionObserver(productions, production));
                workMonitor.start(500);
            }
        }
    }

    private Widget createLevel2Panel() {
        inputPsListBox = new ListBox();
        inputPsListBox.setName("productSetListBox");
        for (PortalProductSet productSet : productSets) {
            inputPsListBox.addItem(productSet.getName(), productSet.getId());
        }
        inputPsListBox.setVisibleItemCount(6);

        outputPsTextBox = new TextBox();
        outputPsTextBox.setName("productSetNameBox");

        processorListBox = new ListBox();
        processorListBox.setName("processorListBox");
        for (PortalProcessor processor : processors) {
            processorListBox.addItem(processor.getName(), processor.getId());
        }
        if (processors.length > 0) {
            processorListBox.setSelectedIndex(0);
        }
        processorListBox.setVisibleItemCount(3);
        processorListBox.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                updateProcessorVersionsListBox();
            }
        });

        processorVersionListBox = new ListBox();
        processorVersionListBox.setName("processorVersionListBox");
        if (processors.length > 0) {
            updateProcessorVersionsListBox();
        }
        processorVersionListBox.setVisibleItemCount(3);

        parametersTextArea = new TextArea();
        parametersTextArea.setName("parameterKeyValuesArea");
        parametersTextArea.setCharacterWidth(48);
        parametersTextArea.setVisibleLines(16);

        parametersFileUpload = new FileUpload();
        parametersFileUpload.setName("uploadFormElement");
        parametersFileUpload.addChangeHandler(new FileUploadChangeHandler());

        Button submitButton = new Button("Submit", new SubmitHandler());

        VerticalPanel productSetPanel = new VerticalPanel();
        productSetPanel.setSpacing(4);
        productSetPanel.add(createLabeledWidget("Input Level 1 product set:", inputPsListBox));
        productSetPanel.add(createLabeledWidget("Output Level 2 product set:", outputPsTextBox));

        HorizontalPanel processorPanel = new HorizontalPanel();
        processorPanel.setSpacing(4);
        processorPanel.add(createLabeledWidget("Processor:", processorListBox));
        processorPanel.add(createLabeledWidget("Version:", processorVersionListBox));

        VerticalPanel processorAndParamsPanel = new VerticalPanel();
        processorPanel.setSpacing(4);
        processorAndParamsPanel.add(processorPanel);
        processorAndParamsPanel.add(createLabeledWidget("Processing parameters:", parametersTextArea));
        processorAndParamsPanel.add(createLabeledWidget("Parameter file:", parametersFileUpload));

        FlexTable flexTable = new FlexTable();
        flexTable.getFlexCellFormatter().setVerticalAlignment(0, 0, HasVerticalAlignment.ALIGN_TOP);
        flexTable.getFlexCellFormatter().setVerticalAlignment(0, 1, HasVerticalAlignment.ALIGN_TOP);
        flexTable.getFlexCellFormatter().setHorizontalAlignment(1, 0, HasHorizontalAlignment.ALIGN_RIGHT);
        flexTable.getFlexCellFormatter().setColSpan(1, 0, 2);
        flexTable.ensureDebugId("cwFlexTable");
        flexTable.addStyleName("cw-FlexTable");
        flexTable.setWidth("32em");
        flexTable.setCellSpacing(2);
        flexTable.setCellPadding(2);
        flexTable.setWidget(0, 0, productSetPanel);
        flexTable.setWidget(0, 1, processorAndParamsPanel);
        flexTable.setWidget(1, 0, submitButton);

        form = new FormPanel();
        form.setWidget(flexTable);

        form.addSubmitHandler(new FormPanel.SubmitHandler() {
            public void onSubmit(FormPanel.SubmitEvent event) {
                // todo - check inputs
            }
        });
        form.addSubmitCompleteHandler(new FormPanel.SubmitCompleteHandler() {
            public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
                parametersTextArea.setText(event.getResults());
            }
        });

        return form;
    }

    private VerticalPanel createLabeledWidget(String labelText, Widget widget) {
        VerticalPanel panel = new VerticalPanel();
        panel.setSpacing(2);
        panel.add(new Label(labelText));
        panel.add(widget);
        return panel;
    }

    private Widget createQueryPanel() {
        //Defining a WMSLayer and adding it to a Map
        WMSParams wmsParams = new WMSParams();
        wmsParams.setFormat("image/png");
        wmsParams.setLayers("basic");
        wmsParams.setStyles("");

        WMSOptions wmsLayerParams = new WMSOptions();
        wmsLayerParams.setUntiled();
        wmsLayerParams.setTransitionEffect(TransitionEffect.RESIZE);

        WMS wmsLayer = new WMS(
                "Basic WMS",
                "http://labs.metacarta.com/wms/vmap0",
                wmsParams,
                wmsLayerParams);

        MapOptions defaultMapOptions = new MapOptions();
        //In OL, the map gets PanZoom, Navigation, ArgParser, and Attribution Controls
        //by default. Do removeDefaultControls to remove these.
        //defaultMapOptions.removeDefaultControls();
        defaultMapOptions.setNumZoomLevels(16);
        defaultMapOptions.setProjection("EPSG:4326");

        MapWidget mapWidget = new MapWidget("350px", "350px", defaultMapOptions);
        mapWidget.getMap().addLayers(new Layer[]{wmsLayer});

        //Adding controls to the Map
        mapWidget.getMap().addControl(new PanZoomBar());
        //use NavToolbar instead of deprecated MouseToolbar
        mapWidget.getMap().addControl(new NavToolbar());
        mapWidget.getMap().addControl(new MousePosition());
        mapWidget.getMap().addControl(new LayerSwitcher());
        //Center and Zoom
        double lon = 4.0;
        double lat = 5.0;
        int zoom = 5;
        mapWidget.getMap().setCenter(new LonLat(lon, lat), zoom);

        return mapWidget;
    }

    private Widget createJobsPanel() {
        // Create a CellTable.

        // Set a key provider that provides a unique key for each contact. If key is
        // used to identify contacts when fields (such as the name and address)
        // change.
        CellTable productionTable = new CellTable<PortalProduction>(JOB_KEY_PROVIDER);
        productionTable.setWidth("100%");

        final SelectionModel<PortalProduction> selectionModel = new MultiSelectionModel<PortalProduction>(JOB_KEY_PROVIDER);
        productionTable.setSelectionModel(selectionModel);

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
                } else if (state == WorkStatus.State.DONE) {
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

        productionTable.addColumn(checkColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));
        productionTable.addColumn(nameColumn, "Production Name");
        productionTable.addColumn(statusColumn, "Production Status");
        productionTable.addColumn(resultColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));


        // Connect the table to the data provider.
        productions.addDataDisplay(productionTable);

        return productionTable;
    }

    private void updateProcessorVersionsListBox() {
        processorVersionListBox.clear();
        int selectedIndex = processorListBox.getSelectedIndex();
        PortalProcessor selectedProcessor = processors[selectedIndex];
        String[] versions = selectedProcessor.getVersions();
        for (String version : versions) {
            processorVersionListBox.addItem(version);
        }
        if (versions.length > 0) {
            processorVersionListBox.setSelectedIndex(0);
        }
    }

    private boolean isAllInputDataAvailable() {
        return productSets != null && processors != null;
    }

    private class SubmitHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            int productSetIndex = inputPsListBox.getSelectedIndex();
            int processorIndex = processorListBox.getSelectedIndex();
            int processorVersionIndex = processorVersionListBox.getSelectedIndex();
            PortalProductionRequest request = new PortalProductionRequest(inputPsListBox.getValue(productSetIndex),
                                                                          outputPsTextBox.getText().trim(),
                                                                          processorListBox.getValue(processorIndex),
                                                                          processorVersionListBox.getValue(processorVersionIndex),
                                                                          parametersTextArea.getText().trim());
            backendService.orderProduction(request, new AsyncCallback<PortalProductionResponse>() {
                public void onSuccess(final PortalProductionResponse response) {
                    tabPanel.selectTab(2);
                    WorkMonitor workMonitor = new WorkMonitor(new ProductionReporter(backendService, response.getProduction()),
                                               new ProductionObserver(productions, response.getProduction()));
                    workMonitor.start(500);
                }

                public void onFailure(Throwable caught) {
                    Window.alert("Error!\n" + caught.getMessage());
                }
            });
        }

    }

    private class FileUploadChangeHandler implements ChangeHandler {

        public void onChange(ChangeEvent event) {
            String filename = parametersFileUpload.getFilename();
            if (filename != null && !filename.isEmpty()) {
                // Because we're going to add a FileUpload widget, we'll need to set the
                // form to use the POST method, and multi-part MIME encoding.
                form.setAction(UPLOAD_ACTION_URL);
                form.setEncoding(FormPanel.ENCODING_MULTIPART);
                form.setMethod(FormPanel.METHOD_POST);
                form.submit();
            }
        }
    }

    /**
     * The key provider that provides the unique ID of a contact.
     */
    public static final ProvidesKey<PortalProduction> JOB_KEY_PROVIDER = new ProvidesKey<PortalProduction>() {
        public Object getKey(PortalProduction item) {
            return item == null ? null : item.getId();
        }
    };

}



