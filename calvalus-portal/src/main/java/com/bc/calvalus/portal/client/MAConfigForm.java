package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.dom.client.Document;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.event.dom.client.DomEvent;
import com.google.gwt.maps.client.MapOptions;
import com.google.gwt.maps.client.MapWidget;
import com.google.gwt.maps.client.base.LatLng;
import com.google.gwt.maps.client.base.LatLngBounds;
import com.google.gwt.maps.client.overlays.Marker;
import com.google.gwt.maps.client.overlays.MarkerImage;
import com.google.gwt.maps.client.overlays.MarkerOptions;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Form for MA (matup analysis) parameters.
 *
 * @author Norman
 */
public class MAConfigForm extends Composite {

    private static final String POINT_DATA_DIR = "point-data";
    public static final String SYSTEM_POINTDATA_DIR = "pointdata";

    private final PortalContext portalContext;
    private final ManagedFiles managedFiles;

    interface TheUiBinder extends UiBinder<Widget, MAConfigForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField(provided = true)
    ListBox recordSources;
    @UiField
    Button addRecordSourceButton;
    @UiField
    Button checkRecordSourceButton;
    @UiField
    Button viewRecordSourceButton;
    @UiField
    Button removeRecordSourceButton;

    @UiField
    IntegerBox macroPixelSize;
    @UiField
    TextBox maxTimeDifference;
    @UiField
    DoubleBox filteredMeanCoeff;
    @UiField
    CheckBox filterOverlapping;
    @UiField
    CheckBox onlyExtractComplete;
    @UiField
    TextBox outputGroupName;

    @UiField
    TextBox goodPixelExpression;
    @UiField
    TextBox goodRecordExpression;
    @UiField
    HTMLPanel expressionTable;


    public MAConfigForm(final PortalContext portalContext) {
        this.portalContext = portalContext;

        // http://stackoverflow.com/questions/22629632/gwt-listbox-onchangehandler
        // fire selection event even when set programmatically
        recordSources = new ListBox() {
            @Override
            public void setSelectedIndex(int index) {
                super.setSelectedIndex(index);
                DomEvent.fireNativeEvent(Document.get().createChangeEvent(), this);
            }
        };
        initWidget(uiBinder.createAndBindUi(this));

        macroPixelSize.setValue(5);
        maxTimeDifference.setValue("3.0");
        filteredMeanCoeff.setValue(1.5);
        filterOverlapping.setValue(false);
        onlyExtractComplete.setValue(true);
        outputGroupName.setValue("SITE");

        HTML description1 = new HTML("The supported file types are TAB-separated CSV (<b>*.txt</b>, <b>*.csv</b>)<br/>" +
                                     "and SNAP placemark files (<b>*.placemark</b>).");
        HTML description2 = new HTML(
                "<h4>Standard format:</h4>" +
                "The first line of the TAB-separated CSV file must contain header names. <br/>" +
                "At least 'LATITUDE' and 'LONGITUDE' must be given, 'TIME' is needed for <br/>" +
                "application of the max. time difference criterion. The time information <br/>" +
                "has be in the format 'yyyy-MM-dd HH:mm:ss'. Other names will be<br/>" +
                "matched against names in the resulting L2 products in order to generate<br/>" +
                "the match-up scatter-plots and statistics, for example 'CONC_CHL'." +
                "<h4>Custom format:</h4>" +
                "If the file deviates from the standard format header lines<br/>" +
                "following the '# &lt;key&gt;=&lt;value&gt;' syntax can added to the file to customize the format:<br/>" +
                "<ol>" +
                "<li><b>columnSeparator</b> The character separating the columns.</li>" +
                "<li><b>latColumn</b> The name of the column containing the latitude.</li>" +
                "<li><b>lonColumn</b> The name of the column containing the longitude.</li>" +
                "<li><b>timeColumn</b> The name of the column containing the time.</li>" +
                "<li><b>timeColumns</b> A comma separated list of column names.<br/>" +
                "The content will be concatenated separated by COMMA.</li>" +
                "<li><b>dateFormat</b> The <a href=\"http://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html\">dateformat</a> for parsing the time.</li>" +
                "</ol>");

        managedFiles = new ManagedFiles(portalContext.getBackendService(),
                                        recordSources,
                                        POINT_DATA_DIR,
                                        "in-situ or point data",
                                        description1,
                                        description2);
        managedFiles.setAddButton(addRecordSourceButton);
        managedFiles.setRemoveButton(removeRecordSourceButton);
        recordSources.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                boolean hasSelection = recordSources.getSelectedIndex() != -1;
                checkRecordSourceButton.setEnabled(hasSelection);
                viewRecordSourceButton.setEnabled(hasSelection);
            }
        });
        checkRecordSourceButton.addClickHandler(new CheckRecordSourceAction());
        checkRecordSourceButton.setEnabled(false);
        viewRecordSourceButton.addClickHandler(new ViewRecordSourceAction());
        viewRecordSourceButton.setEnabled(false);
        managedFiles.loadSystemFiles(SYSTEM_POINTDATA_DIR,"system point data");
        managedFiles.updateUserFiles(true);
    }

    private void checkRecordSource(final String recordSource) {
        portalContext.getBackendService().checkUserRecordSource(recordSource, new AsyncCallback<String>() {
            @Override
            public void onSuccess(String message) {
                Dialog.info("Passed", "parsing " + recordSource + " succeeded: " + message);
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Failed", "Failed to parse " + recordSource + ": " + caught.getMessage());
            }
        });
    }

    private void viewRecordSource(final String recordSource) {
        portalContext.getBackendService().listUserRecordSource(recordSource, new AsyncCallback<float[]>() {
            @Override
            public void onSuccess(float[] points) {
                MapOptions mapOptions = MapOptions.newInstance();
                mapOptions.setCenter(LatLng.newInstance(0.0, 0.0));
                mapOptions.setDisableDoubleClickZoom(false);
                mapOptions.setScrollWheel(true);
                mapOptions.setMapTypeControl(true);
                mapOptions.setZoomControl(true);
                mapOptions.setPanControl(true);
                mapOptions.setStreetViewControl(false);
                final MapWidget mapWidget = new MapWidget(mapOptions);
                mapWidget.setSize("800px", "520px");
                LatLngBounds bounds = null;
                MarkerImage markerImage = MarkerImage.newInstance("https://maps.gstatic.com/intl/en_ALL/mapfiles/markers2/measle.png");
                for (int i = 0; i < points.length; ) {
                    final float lat = points[i++];
                    final float lon = points[i++];
                    LatLng latLngPoint = LatLng.newInstance(lat, lon);
                    if (bounds == null) {
                        bounds = LatLngBounds.newInstance(latLngPoint, latLngPoint);
                    } else {
                        bounds.extend(latLngPoint);
                    }
                    MarkerOptions markerOptions = MarkerOptions.newInstance();
                    markerOptions.setPosition(latLngPoint);
                    Marker marker = Marker.newInstance(markerOptions);
                    marker.setIcon(markerImage);
                    marker.setMap(mapWidget);
                }
                if (bounds != null) {
                    final LatLngBounds finalBounds = bounds;
                    String title = "Viewing " + recordSource + " with " + (points.length / 2) + " measurements";
                    Dialog dialog = new Dialog(title, mapWidget, Dialog.ButtonType.CLOSE) {
                        @Override
                        protected void onShow() {
                            mapWidget.triggerResize();
                            mapWidget.fitBounds(finalBounds);
                            mapWidget.panTo(finalBounds.getCenter());
                        }
                    };
                    dialog.show();
                }
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Failed", "Failed to view " + recordSource + ": " + caught.getMessage());
            }
        });
    }

    public void validateForm() throws ValidationException {
        Integer macroPixelSizeValue = macroPixelSize.getValue();
        boolean macroPixelSizeValid = macroPixelSizeValue != null
                                      && macroPixelSizeValue >= 1
                                      && macroPixelSizeValue <= 201
                                      && macroPixelSizeValue % 2 == 1;
        if (!macroPixelSizeValid) {
            throw new ValidationException(macroPixelSize, "Macro pixel size must be an odd integer between 1 and 201");
        }
        boolean filteredMeanCoeffValid = filteredMeanCoeff.getValue() != null && filteredMeanCoeff.getValue() >= 0;
        if (!filteredMeanCoeffValid) {
            throw new ValidationException(filteredMeanCoeff, "Filtered mean coefficient must be >= 0 (0 disables this criterion)");
        }

        if (!isMaxTimeDifferenceValid(maxTimeDifference.getValue())) {
            throw new ValidationException(maxTimeDifference, "Max. time difference must be >= 0 hours (0 disables this criterion).<br/>" +
                    "Alternatively the difference can be given in full days using the 'd' suffix e.g. 0d,1d,...");
        }

        if (managedFiles.getSelectedFilePath().isEmpty()) {
            throw new ValidationException(maxTimeDifference, "In-situ record source must be given.");
        }
    }
    
    private boolean isMaxTimeDifferenceValid(String maxTimeDifference) throws ValidationException {
        if (maxTimeDifference == null || maxTimeDifference.trim().isEmpty()) {
            return true;
        }
        maxTimeDifference = maxTimeDifference.trim();
        if (maxTimeDifference.endsWith("d") && maxTimeDifference.length() >= 2) {
            String daysAsString = maxTimeDifference.substring(0, maxTimeDifference.length() - 1);
            try {
                int days = Integer.parseInt(daysAsString);
                return days >= 0;
            } catch (NumberFormatException nfe) {
                return false;
            }
        } else {
            try {
                return Double.parseDouble(maxTimeDifference) >= 0;
            } catch (NumberFormatException nfe) {
                return false;
            }
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("copyInput", "true");
        parameters.put("goodPixelExpression", ParametersEditorGenerator.encodeXML(goodPixelExpression.getText()));
        parameters.put("goodRecordExpression", ParametersEditorGenerator.encodeXML(goodRecordExpression.getText()));
        parameters.put("macroPixelSize", macroPixelSize.getValue().toString());
        parameters.put("maxTimeDifference", maxTimeDifference.getText());
        parameters.put("filteredMeanCoeff", filteredMeanCoeff.getValue().toString());
        parameters.put("filterOverlapping", filterOverlapping.getValue().toString());
        parameters.put("onlyExtractComplete", onlyExtractComplete.getValue().toString());
        parameters.put("outputGroupName", outputGroupName.getText());
        parameters.put("recordSourceUrl", managedFiles.getSelectedFilePath());
        return parameters;
    }

    public void setValues(Map<String, String> parameters) {
        goodPixelExpression.setValue(ParametersEditorGenerator.decodeXML(parameters.getOrDefault("goodPixelExpression", "")));
        goodRecordExpression.setValue(ParametersEditorGenerator.decodeXML(parameters.getOrDefault("goodRecordExpression", "")));
        macroPixelSize.setText(parameters.getOrDefault("macroPixelSize", "5"));
        maxTimeDifference.setValue(parameters.getOrDefault("maxTimeDifference", "3.0"));
        filteredMeanCoeff.setText(parameters.getOrDefault("filteredMeanCoeff", "1.5"));
        filterOverlapping.setValue(Boolean.valueOf(parameters.getOrDefault("filterOverlapping", "false")));
        onlyExtractComplete.setValue(Boolean.valueOf(parameters.getOrDefault("onlyExtractComplete", "true")));
        outputGroupName.setValue(parameters.getOrDefault("outputGroupName", ""));
        String recordSourceUrl = parameters.get("recordSourceUrl");
        if (recordSourceUrl != null) {
            managedFiles.setSelectedFilePath(recordSourceUrl);
        }
    }


    private class CheckRecordSourceAction implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            // should be made async!
            String selectedFilePath = managedFiles.getSelectedFilePath();
            if (selectedFilePath.isEmpty()) {
                Dialog.error("Check record source",
                                             "No file selected.");
            } else {
                checkRecordSource(selectedFilePath);
            }
        }
    }

    private class ViewRecordSourceAction implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            // should be made async!
            String selectedFilePath = managedFiles.getSelectedFilePath();
            if (selectedFilePath.isEmpty()) {
                Dialog.error("View record source",
                                             "No file selected.");
            } else {
                viewRecordSource(selectedFilePath);
            }
        }
    }

}
