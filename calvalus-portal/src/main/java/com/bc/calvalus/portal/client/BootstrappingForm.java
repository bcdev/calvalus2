package com.bc.calvalus.portal.client;

import com.google.gwt.core.client.GWT;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.IntegerBox;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.TextBox;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Marco Peters
 */
public class BootstrappingForm extends Composite {

    private static final int DEFAULT_NUMBER_OF_ITERATIONS = 10000;
    private final UserManagedFiles userManagedContent;


    interface TheUiBinder extends UiBinder<Widget, BootstrappingForm> {

    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    @UiField
    ListBox bootstrapSources;
    @UiField
    Button addBootstrapSourceButton;
    @UiField
    Button removeBootstrapSourceButton;

    @UiField
    ListBox processorList;
    @UiField
    IntegerBox numberOfIterations;
    @UiField
    TextBox productionName;

    public BootstrappingForm(PortalContext portalContext) {
        initWidget(uiBinder.createAndBindUi(this));

        numberOfIterations.setValue(DEFAULT_NUMBER_OF_ITERATIONS);

        final String fileExtension = ".csv";
        HTML description = new HTML("The supported file types are TAB-separated CSV (<b>*" + fileExtension + "</b>) matchup files.<br/>");
        final String baseDir = "bootstrapping";
        userManagedContent = new UserManagedFiles(portalContext.getBackendService(),
                                                  bootstrapSources,
                                                  baseDir,
                                                  "matchup",
                                                  description);
        userManagedContent.setFilePathFilter(new Filter<String>() {
            @Override
            public boolean accept(String filePath) {
                int lastSlashIndex = filePath.lastIndexOf("/");
                boolean inBaseDir = false;
                if (lastSlashIndex >= baseDir.length()) {
                    String substring = filePath.substring(lastSlashIndex - baseDir.length(), lastSlashIndex);
                    inBaseDir = baseDir.equals(substring);
                }
                return filePath.endsWith(fileExtension) && inBaseDir;
            }
        });
        addBootstrapSourceButton.addClickHandler(userManagedContent.getAddAction());
        removeBootstrapSourceButton.addClickHandler(userManagedContent.getRemoveAction());
        userManagedContent.updateList();
    }

    public void validateForm() throws ValidationException {

        Integer numberOfIterationsValue = numberOfIterations.getValue();
        if (numberOfIterationsValue == null || numberOfIterationsValue <= 0) {
            throw new ValidationException(numberOfIterations, "Number of Iterations must be > 0");
        }

        boolean bootstrapSourceValid = bootstrapSources.getSelectedIndex() >= 0;
        if (!bootstrapSourceValid) {
            throw new ValidationException(bootstrapSources, "Bootstrap source must be given.");
        }
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put("calvalus.bootstrap.numberOfIterations", numberOfIterations.getText());
        parameters.put("calvalus.bootstrap.inputFile", userManagedContent.getSelectedFilename());
        parameters.put("productionName", productionName.getValue());
        return parameters;
    }

}
