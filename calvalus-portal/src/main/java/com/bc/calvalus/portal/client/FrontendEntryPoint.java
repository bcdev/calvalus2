package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.PortalProcessor;
import com.bc.calvalus.portal.shared.PortalProductSet;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.google.gwt.cell.client.ActionCell;
import com.google.gwt.cell.client.CheckboxCell;
import com.google.gwt.cell.client.NumberCell;
import com.google.gwt.cell.client.TextCell;
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
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.DecoratedTabPanel;
import com.google.gwt.user.client.ui.DialogBox;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;
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
public class FrontendEntryPoint implements EntryPoint {

    static final String UPLOAD_ACTION_URL = GWT.getModuleBaseURL() + "upload";

    private final BackendServiceAsync backendService = GWT.create(BackendService.class);

    private FormPanel form;
    private ListBox processorListBox;
    private ListBox processorVersionListBox;
    private ListBox inputPsListBox;
    private TextArea parametersTextArea;
    private FileUpload parametersFileUpload;
    private TextBox outputPsTextBox;

    private PortalProductSet[] productSets;
    private PortalProcessor[] processors;
    private boolean initialised;
    private DecoratedTabPanel tabPanel;

    /**
     * This is the entry point method.
     */
    @Override
    public void onModuleLoad() {

        final DialogBox splashScreen = createSplashScreen();

        backendService.getProductSets(null, new AsyncCallback<PortalProductSet[]>() {
            @Override
            public void onSuccess(PortalProductSet[] productSets) {
                FrontendEntryPoint.this.productSets = productSets;
                maybeInitFrontend(splashScreen);
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Error!\n" + caught.getMessage());
                FrontendEntryPoint.this.productSets = new PortalProductSet[0];
            }
        });

        backendService.getProcessors(null, new AsyncCallback<PortalProcessor[]>() {
            @Override
            public void onSuccess(PortalProcessor[] processors) {
                FrontendEntryPoint.this.processors = processors;
                maybeInitFrontend(splashScreen);
            }

            @Override
            public void onFailure(Throwable caught) {
                Window.alert("Error!\n" + caught.getMessage());
                FrontendEntryPoint.this.processors = new PortalProcessor[0];
            }
        });
    }

    private DialogBox createSplashScreen() {
        final DialogBox splashScreen = new DialogBox();
        splashScreen.setText("Calvalus");
        splashScreen.add(new Label("Please wait, while Calvalus is loading..."));
        splashScreen.setModal(true);
        splashScreen.setAnimationEnabled(true);
        splashScreen.setGlassEnabled(true);
        splashScreen.setSize("320", "200");
        splashScreen.showRelativeTo(RootPanel.get());
        return splashScreen;
    }


    private void maybeInitFrontend(DialogBox splashScreen) {
        if (!initialised && isAllInputDataAvailable()) {
            initialised = true;
            splashScreen.hide();
            DOM.removeChild(RootPanel.getBodyElement(), DOM.getElementById("loading"));
            initFrontend();
        }
    }

    private void initFrontend() {
        tabPanel = new DecoratedTabPanel();
        tabPanel.setWidth("400px");
        tabPanel.setAnimationEnabled(true);

        tabPanel.add(createQueryPanel(), "Query");
        tabPanel.add(createLevel2Panel(), "Level 2");
        tabPanel.add(createJobsPanel(), "Jobs");

        tabPanel.selectTab(1);
        tabPanel.ensureDebugId("cwTabPanel");

        RootPanel.get("mainPanel").add(tabPanel);
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


        VerticalPanel productSetPanelA = new VerticalPanel();
        productSetPanelA.setSpacing(4);
        productSetPanelA.add(new Label("Input Level 1 product set:"));
        productSetPanelA.add(inputPsListBox);
        VerticalPanel productSetPanelB = new VerticalPanel();
        productSetPanelB.setSpacing(4);
        productSetPanelB.add(new Label("Output Level 2 product set:"));
        productSetPanelB.add(outputPsTextBox);
        HorizontalPanel productSetPanel = new HorizontalPanel();
        productSetPanel.setSpacing(4);
        productSetPanel.add(productSetPanelA);
        productSetPanel.add(productSetPanelB);

        VerticalPanel processorPanelA = new VerticalPanel();
        processorPanelA.setSpacing(4);
        processorPanelA.add(new Label("Processor:"));
        processorPanelA.add(processorListBox);
        VerticalPanel processorPanelB = new VerticalPanel();
        processorPanelB.setSpacing(4);
        processorPanelB.add(new Label("Version:"));
        processorPanelB.add(processorVersionListBox);
        HorizontalPanel processorPanel = new HorizontalPanel();
        processorPanel.setSpacing(4);
        processorPanel.add(processorPanelA);
        processorPanel.add(processorPanelB);

        // Create a panel to hold all of the form widgets.
        final VerticalPanel formPanel = new VerticalPanel();
        formPanel.setStyleName("formPanel");

        formPanel.add(productSetPanel);
        formPanel.add(processorPanel);
        formPanel.add(new Label("Processing parameters:"));
        formPanel.add(parametersTextArea);
        formPanel.add(new Label("Parameter file:"));
        formPanel.add(parametersFileUpload);
        formPanel.add(submitButton);

        form = new FormPanel();
        form.setWidget(formPanel);

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
        CellTable cellTable = new CellTable<JobInfo>(JOB_KEY_PROVIDER);
        cellTable.setWidth("100%");


        final SelectionModel<JobInfo> selectionModel = new MultiSelectionModel<JobInfo>(JOB_KEY_PROVIDER);
        cellTable.setSelectionModel(selectionModel);

        Column<JobInfo, Boolean> checkColumn = new Column<JobInfo, Boolean>(new CheckboxCell(true)) {
            @Override
            public Boolean getValue(JobInfo object) {
                // Get the value from the selection model.
                return selectionModel.isSelected(object);
            }
        };
        cellTable.addColumn(checkColumn, SafeHtmlUtils.fromSafeConstant("<br/>"));

        // First name.
        Column<JobInfo, String> nameColumn = new Column<JobInfo, String>(new TextCell()) {
            @Override
            public String getValue(JobInfo object) {
                return object.getName();
            }
        };

        cellTable.addColumn(nameColumn, "Production Name");

        // Last name.
        Column<JobInfo, Number> percentColumn = new Column<JobInfo, Number>(new NumberCell()) {
            @Override
            public Number getValue(JobInfo object) {
                return object.getPercent();
            }
        };
        cellTable.addColumn(percentColumn,  "Percent complete");

        ActionCell actionCell = new ActionCell("see",
                                         new ActionCell.Delegate() {
                                             @Override
                                             public void execute(Object object) {
                                                 // TODO
                                                  Window.alert("Here are your results:");
                                             }
                                         }
        );
        Column<JobInfo, JobInfo> resultColumn = new IdentityColumn<JobInfo>(actionCell);
        cellTable.addColumn(resultColumn,  "Result");

        List<JobInfo> jobInfoList = Arrays.asList(
                new JobInfo("id1", "name1", 5),
                new JobInfo("id2", "name2", 15),
                new JobInfo("id3", "name3", 25)
        );
        cellTable.setRowData(0, jobInfoList);
        return cellTable;
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
                    Worker worker = new Worker(new ProductionReporter(backendService, response),
                                               new ProductionObserver(response));
                    worker.start(500);
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
    public static final ProvidesKey<JobInfo> JOB_KEY_PROVIDER = new ProvidesKey<JobInfo>() {
        public Object getKey(JobInfo item) {
            return item == null ? null : item.getId();
        }
    };

    public static class JobInfo {
        private String id;
        String name;
        int percent;

        public JobInfo(String id, String name, int percent) {
            this.id = id;
            this.name = name;
            this.percent = percent;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public int getPercent() {
            return percent;
        }
    }

}



