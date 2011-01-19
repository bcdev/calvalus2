package com.bc.calvalus.ui.client;

import com.bc.calvalus.ui.shared.ProcessingRequest;
import com.google.gwt.core.client.EntryPoint;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.TextArea;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.VerticalPanel;
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

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class CalvalusApp implements EntryPoint {

    static final String PROCESS_ACTION_URL = GWT.getModuleBaseURL() + "process";
    static final String UPLOAD_ACTION_URL = GWT.getModuleBaseURL() + "upload";

    private final ProcessingServiceAsync processingService = GWT.create(ProcessingService.class);

    private FormPanel form;
    private ListBox processorListBox;
    private ListBox inputPsListBox;
    private TextArea parametersTextArea;
    private FileUpload parametersFileUpload;
    private TextBox outputPsTextBox;

    /**
     * This is the entry point method.
     */
    public void onModuleLoad() {

        inputPsListBox = new ListBox();
        inputPsListBox.setName("productSetListBox");
        inputPsListBox.addItem("MERIS RR 2004-2009");
        inputPsListBox.addItem("MERIS RR 2004");
        inputPsListBox.addItem("MERIS RR 2005");
        inputPsListBox.addItem("MERIS RR 2006");
        inputPsListBox.addItem("MERIS RR 2007");
        inputPsListBox.addItem("MERIS RR 2008");
        inputPsListBox.addItem("MERIS RR 2009");
        inputPsListBox.setVisibleItemCount(1);

        outputPsTextBox = new TextBox();
        outputPsTextBox.setName("productSetNameBox");

        processorListBox = new ListBox();
        processorListBox.setName("processorListBox");
        processorListBox.addItem("Case 2 Regional");
        processorListBox.addItem("QAA");
        processorListBox.setVisibleItemCount(1);

        parametersTextArea = new TextArea();
        parametersTextArea.setName("parameterKeyValuesArea");
        parametersTextArea.setCharacterWidth(48);
        parametersTextArea.setVisibleLines(16);

        parametersFileUpload = new FileUpload();
        parametersFileUpload.setName("uploadFormElement");
        parametersFileUpload.addChangeHandler(new FileUploadChangeHandler());

        Button submitButton = new Button("Submit", new SubmitHandler());


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

		MapWidget mapWidget = new MapWidget("350px","350px", defaultMapOptions);
        mapWidget.getMap().addLayers(new Layer[] {wmsLayer});

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

        // Create a panel to hold all of the form widgets.
        final VerticalPanel formPanel = new VerticalPanel();
        formPanel.setStyleName("formPanel");
        formPanel.add(new HTML("<b>Input Level 1 product set:</b>"));
        formPanel.add(inputPsListBox);
        formPanel.add(new HTML("<b>Output Level 2 product set:</b>"));
        formPanel.add(outputPsTextBox);
        formPanel.add(new HTML("<b>Level-2 Processor:</b>"));
        formPanel.add(processorListBox);
        formPanel.add(new HTML("<b>Processing parameters:</b>"));
        formPanel.add(parametersTextArea);
        formPanel.add(new HTML("<b>Parameter file:</b>"));
        formPanel.add(parametersFileUpload);
        formPanel.add(submitButton);
        formPanel.add(new HTML("<br/><br/><b>Map:</b>"));
        formPanel.add(mapWidget);


        form = new FormPanel();
        form.setWidget(formPanel);
        form.addSubmitHandler(new FormPanel.SubmitHandler() {
            public void onSubmit(FormPanel.SubmitEvent event) {
                // todo - check inputs
                //Window.alert(GWT.getModuleBaseURL());
            }
        });
        form.addSubmitCompleteHandler(new FormPanel.SubmitCompleteHandler() {
            public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
                parametersTextArea.setText(event.getResults());
            }
        });

        RootPanel.get().add(form);
    }

    private class SubmitHandler implements ClickHandler {

        public void onClick(ClickEvent event) {
            int processorIndex = processorListBox.getSelectedIndex();
            int productSetIndex = inputPsListBox.getSelectedIndex();
            ProcessingRequest request = new ProcessingRequest(inputPsListBox.getValue(productSetIndex),
                                                              outputPsTextBox.getText(),
                                                              processorListBox.getValue(processorIndex),
                                                              parametersTextArea.getText());
            processingService.process(request, new AsyncCallback<String>() {
                public void onSuccess(String result) {
                    Window.alert("Success!\n" + result);
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
}



