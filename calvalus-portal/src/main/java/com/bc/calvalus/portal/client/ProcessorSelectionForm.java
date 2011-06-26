package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessorDescriptor;
import com.google.gwt.core.client.GWT;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
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

    public void addProcessorChangedHandler(final ProcessorChangedHandler changedHandler) {
        processorList.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent changeEvent) {
                changedHandler.onProcessorChanged(getSelectedProcessor());
            }
        });
    }

    public DtoProcessorDescriptor getSelectedProcessor() {
        int selectedIndex = processorList.getSelectedIndex();
        return processorDescriptors[selectedIndex];
    }


    public void validateForm() throws ValidationException {
    }

    public static interface ProcessorChangedHandler {
        void onProcessorChanged(DtoProcessorDescriptor processorDescriptor);
    }

}