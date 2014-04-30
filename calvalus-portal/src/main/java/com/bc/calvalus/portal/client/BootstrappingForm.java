package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.processing.boostrapping.BootstrappingWorkflowItem;
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

    @UiField(provided = true)
    L2ConfigForm l2ConfigForm;
    @UiField
    IntegerBox numberOfIterations;
    @UiField
    TextBox productionName;

    public BootstrappingForm(PortalContext portalContext) {
        l2ConfigForm = new L2ConfigForm(portalContext, new BootstrappingFilter(), true);
        l2ConfigForm.processorListLabel.setText("Processor");
        l2ConfigForm.parametersLabel.setText("Parameters");
        initWidget(uiBinder.createAndBindUi(this));

        numberOfIterations.setValue(DEFAULT_NUMBER_OF_ITERATIONS);

        final String fileExtension = ".csv";
        final String baseDir = "bootstrapping";
        HTML description = new HTML("The supported file types are TAB-separated CSV (<b>*" + fileExtension + "</b>) matchup files.<br/>");
        userManagedContent = new UserManagedFiles(portalContext.getBackendService(),
                                                  bootstrapSources,
                                                  baseDir,
                                                  "matchup",
                                                  description);
        addBootstrapSourceButton.addClickHandler(userManagedContent.getAddAction());
        removeBootstrapSourceButton.addClickHandler(userManagedContent.getRemoveAction());
        userManagedContent.updateList();

        //TODO filter bootstrap processor(s) in all other views
    }

    public void validateForm() throws ValidationException {
        Integer numberOfIterationsValue = numberOfIterations.getValue();
        if (numberOfIterationsValue == null || numberOfIterationsValue <= 0) {
            throw new ValidationException(numberOfIterations, "Number of Iterations must be > 0");
        }

        boolean bootstrapSourceValid = userManagedContent.getSelectedFilePath() != null;
        if (!bootstrapSourceValid) {
            throw new ValidationException(bootstrapSources, "Bootstrap source must be given.");
        }
        l2ConfigForm.validateForm();
    }

    public Map<String, String> getValueMap() {
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.putAll(l2ConfigForm.getValueMap());

        parameters.put(BootstrappingWorkflowItem.NUM_ITERATIONS_PROPERTY, numberOfIterations.getValue().toString());
        parameters.put(BootstrappingWorkflowItem.INPUT_FILE_PROPRTY, userManagedContent.getSelectedFilePath());
        if (!productionName.getValue().isEmpty()) {
            parameters.put("productionName", productionName.getValue());
        }
        parameters.put("autoStaging", String.valueOf(true));
        return parameters;
    }

    private static class BootstrappingFilter implements Filter<DtoProcessorDescriptor> {
        @Override
        public boolean accept(DtoProcessorDescriptor dtoProcessorDescriptor) {
            return dtoProcessorDescriptor.getProcessorCategory() == DtoProcessorDescriptor.DtoProcessorCategory.BOOTSTRAPPING;
        }
    }
}
