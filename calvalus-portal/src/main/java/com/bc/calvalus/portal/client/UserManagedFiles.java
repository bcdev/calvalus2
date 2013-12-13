package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Manages a list of files that can be uploaded and remove by the user.
 */
class UserManagedFiles {

    private final BackendServiceAsync backendService;
    private final ListBox contentListbox;
    private final String baseDir;
    private final String what;
    private final FileUpload fileUpload;
    private final FormPanel uploadForm;
    private final RemoveAction removeAction;
    private final AddAction addAction;
    private final Widget[] uploadDescriptions;

    UserManagedFiles(BackendServiceAsync backendService, ListBox contentListbox, String baseDir, String what, Widget... uploadDescriptions) {
        this.backendService = backendService;
        this.contentListbox = contentListbox;
        this.baseDir = baseDir;
        this.what = what;
        this.uploadDescriptions = uploadDescriptions;

        fileUpload = new FileUpload();
        fileUpload.setName("fileUpload");
        uploadForm = new FormPanel();
        uploadForm.setWidget(fileUpload);

        addAction = new AddAction();
        removeAction = new RemoveAction();

        FileUploadManager.configureForm(uploadForm,
                                        "dir=" + baseDir,
                                        addAction,
                                        addAction);
    }


    public void updateList() {
        backendService.listUserFiles(baseDir, new AsyncCallback<String[]>() {
            @Override
            public void onSuccess(String[] filePaths) {
                setItems(filePaths);
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error", "Failed to get list of " + what + " files from server.");
            }
        });
    }

    private void removeRecordSource(final String fileToRemove) {
        backendService.removeUserFile(baseDir + "/" + fileToRemove, new AsyncCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean removed) {
                updateList();
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error", "Failed to remove file '" + fileToRemove + "' from server.");
            }
        });
    }

    private void setItems(String[] filePaths) {
        contentListbox.clear();
        for (String filePath : filePaths) {
            int baseDirPos = filePath.lastIndexOf(baseDir + "/");
            if (baseDirPos >= 0) {
                contentListbox.addItem(filePath.substring(baseDirPos + baseDir.length() + 1), filePath);
            } else {
                contentListbox.addItem(filePath, filePath);
            }
        }
        if (contentListbox.getItemCount() > 0) {
            contentListbox.setSelectedIndex(0);
        }
    }

    public String getSelectedFilePath() {
        int selectedIndex = contentListbox.getSelectedIndex();
        return selectedIndex >= 0 ? contentListbox.getValue(selectedIndex) : "";
    }

    ClickHandler getRemoveAction() {
        return removeAction;
    }

    ClickHandler getAddAction() {
        return addAction;
    }


    public String getSelectedFilename() {
        int selectedIndex = contentListbox.getSelectedIndex();
        if (selectedIndex >= 0) {
            return contentListbox.getItemText(selectedIndex);
        }
        return null;
    }

    private class AddAction implements ClickHandler, FormPanel.SubmitHandler, FormPanel.SubmitCompleteHandler {

        private Dialog fileUploadDialog;
        private Dialog monitorDialog;
        private FormPanel.SubmitEvent submitEvent;

        @Override
        public void onClick(ClickEvent event) {
            List<Widget> widgetList = new ArrayList<Widget>();
            widgetList.add(new HTML("Select " + what + " file:"));
            widgetList.add(uploadForm);
            Collections.addAll(widgetList, uploadDescriptions);
            Widget[] widgets = widgetList.toArray(new Widget[widgetList.size()]);
            VerticalPanel verticalPanel = UIUtils.createVerticalPanel(2, widgets);
            fileUploadDialog = new Dialog("File Upload", verticalPanel, Dialog.ButtonType.OK, Dialog.ButtonType.CANCEL) {
                @Override
                protected void onOk() {
                    String filename = fileUpload.getFilename();
                    if (filename == null || filename.isEmpty()) {
                        Dialog.info("File Upload",
                                    new HTML("No filename selected."),
                                    new HTML("Please specify a " + what + "file."));
                        return;
                    }
                    monitorDialog = new Dialog("File Upload", new Label("Submitting '" + filename + "'..."), ButtonType.CANCEL) {
                        @Override
                        protected void onCancel() {
                            cancelSubmit();
                        }
                    };
                    monitorDialog.show();
                    uploadForm.submit();
                }
            };

            fileUploadDialog.show();
        }

        private void cancelSubmit() {
            closeDialogs();
            if (submitEvent != null) {
                submitEvent.cancel();
            }
        }

        @Override
        public void onSubmit(FormPanel.SubmitEvent event) {
            this.submitEvent = event;
        }

        @Override
        public void onSubmitComplete(FormPanel.SubmitCompleteEvent event) {
            closeDialogs();
            updateList();
            Dialog.info("File Upload", "File successfully uploaded.");
        }

        private void closeDialogs() {
            monitorDialog.hide();
            fileUploadDialog.hide();
        }
    }


    private class RemoveAction implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            final String recordSource = getSelectedFilename();
            if (recordSource != null) {
                Dialog.ask("Remove File",
                           new HTML("The file '" + recordSource + "' will be permanently deleted.<br/>" +
                                    "Do you really want to continue?"),
                           new Runnable() {
                               @Override
                               public void run() {
                                   removeRecordSource(recordSource);
                               }
                           });
            } else {
                Dialog.error("Remove File",
                             "No file selected.");
            }
        }
    }
}
