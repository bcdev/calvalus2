package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.bc.calvalus.portal.shared.DtoProductSet;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.uibinder.client.UiBinder;
import com.google.gwt.uibinder.client.UiField;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.Widget;

/**
 * Demo view that lets users submit a new L2 production.
 *
 * @author Marco
 * @author Norman
 */
public class ProcessorSelectionForm extends Composite {


    interface TheUiBinder extends UiBinder<Widget, ProcessorSelectionForm> {
    }

    private static TheUiBinder uiBinder = GWT.create(TheUiBinder.class);

    private DtoProcessorDescriptor[] processorDescriptors;

    @UiField
    HTML title;
    @UiField
    ListBox processorList;

    public ProcessorSelectionForm(DtoProcessorDescriptor[] processorDescriptors, String title) {
        initWidget(uiBinder.createAndBindUi(this));

        this.processorDescriptors = processorDescriptors;

        this.title.setHTML(title);
        for (DtoProcessorDescriptor processor : processorDescriptors) {
            String label = processor.getBundleName() + "-" + processor.getBundleVersion();
            this.processorList.addItem(label);
        }
        if (processorDescriptors.length > 0) {
            processorList.setSelectedIndex(0);
        }
    }

    public void addChangeHandler(final ChangeHandler changeHandler) {
        processorList.addChangeHandler(new com.google.gwt.event.dom.client.ChangeHandler() {
            @Override
            public void onChange(ChangeEvent changeEvent) {
                changeHandler.onProcessorChanged(getSelectedProcessor());
            }
        });
    }

    public DtoProcessorDescriptor getSelectedProcessor() {
        int selectedIndex = processorList.getSelectedIndex();
        if (selectedIndex >= 0) {
            return processorDescriptors[selectedIndex];
        } else {
            return null;
        }
    }


    public void validateForm() throws ValidationException {
        DtoProcessorDescriptor selectedProcessorDescriptor = getSelectedProcessor();
        boolean inputProductSetIdValid = selectedProcessorDescriptor != null;
        if (!inputProductSetIdValid) {
            throw new ValidationException(processorList, "An processor must be selected.");
        }
    }

    public static interface ChangeHandler {
        void onProcessorChanged(DtoProcessorDescriptor processorDescriptor);
    }

}