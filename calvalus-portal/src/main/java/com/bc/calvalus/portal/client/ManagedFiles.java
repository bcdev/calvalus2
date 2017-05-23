/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de) 
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

import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.google.gwt.event.dom.client.ChangeEvent;
import com.google.gwt.event.dom.client.ChangeHandler;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Element;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.ui.Button;
import com.google.gwt.user.client.ui.FileUpload;
import com.google.gwt.user.client.ui.FormPanel;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Label;
import com.google.gwt.user.client.ui.ListBox;
import com.google.gwt.user.client.ui.VerticalPanel;
import com.google.gwt.user.client.ui.Widget;

import java.util.*;

/**
 * Manages a list of files that can be uploaded and remove by the user.
 */
class ManagedFiles {

    private final BackendServiceAsync backendService;
    private final List<Entry> userEntries;
    private final List<Entry> systemEntries;
    private final ListBox contentListbox;
    private final String userBaseDir;
    private final String what;
    private final FileUpload fileUpload;
    private final FormPanel uploadForm;
    private final AddAction addAction;
    private final Widget[] uploadDescriptions;
    
    private Button removeButton;
    private Button removeAllButton;

    ManagedFiles(BackendServiceAsync backendService, ListBox contentListbox, String userBaseDir, String what, Widget... uploadDescriptions) {
        this.backendService = backendService;
        this.contentListbox = contentListbox;
        this.userEntries = new ArrayList<>();
        this.systemEntries = new ArrayList<>();
        this.userBaseDir = userBaseDir;
        this.what = what;
        this.uploadDescriptions = uploadDescriptions;

        fileUpload = new FileUpload();
        fileUpload.setName("fileUpload");
        uploadForm = new FormPanel();
        uploadForm.setWidget(fileUpload);

        addAction = new AddAction();
        FileUploadManager.configureForm(uploadForm,
                                        "dir=" + userBaseDir,
                                        addAction,
                                        addAction);
        contentListbox.addChangeHandler(new ChangeHandler() {
            @Override
            public void onChange(ChangeEvent event) {
                removeButton.setEnabled(userEntries.size() > 0 && contentListbox.getSelectedIndex() != -1);    
            }
        });
    }
    
    public void setAddButton(Button button) {
        button.addClickHandler(addAction);
    }

    public void setRemoveButton(Button button) {
        button.setEnabled(false);
        button.addClickHandler(new RemoveAction());
        this.removeButton = button;
    }
    
    public void setRemoveAllButton(Button button) {
        button.setEnabled(false);
        button.addClickHandler(new RemoveAllAction());
        this.removeAllButton = button;
    }
    
    public void updateUserFiles(boolean selectedAnEntry) {
        backendService.listUserFiles(userBaseDir, new AsyncCallback<String[]>() {
            @Override
            public void onSuccess(String[] filePaths) {
                updateFileList(userBaseDir, true, filePaths);
                if (selectedAnEntry && contentListbox.getSelectedIndex() == -1 && contentListbox.getItemCount() > 0) {
                    contentListbox.setSelectedIndex(0);
                }
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error", "Failed to get list of " + what + " files from server.");
            }
        });
    }

    public void loadSystemFiles(String systemBaseDir, String what) {
        backendService.listSystemFiles(systemBaseDir, new AsyncCallback<String[]>() {
            @Override
            public void onSuccess(String[] filePaths) {
                updateFileList(systemBaseDir, false, filePaths);
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error", "Failed to get list of " + what + " files from server.");
            }
        });
    }

    private void removeRecordSource(final String fileToRemove) {
        backendService.removeUserFile(userBaseDir + "/" + fileToRemove, new AsyncCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean removed) {
                updateUserFiles(true);
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error", "Failed to remove file '" + fileToRemove + "' from server.");
            }
        });
    }

    private void removeRecordSources(final String...filesToRemove) {
        String[] pathsToRemove = new String[filesToRemove.length];
        for (int i = 0; i < pathsToRemove.length; i++) {
            pathsToRemove[i] = userBaseDir + "/" + filesToRemove[i];
        }
        backendService.removeUserFiles(pathsToRemove, new AsyncCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean removed) {
                updateUserFiles(false);
            }

            @Override
            public void onFailure(Throwable caught) {
                Dialog.error("Error", "Failed to remove '" + pathsToRemove.length + "' files from server.");
            }
        });
    }

    // javascript to add tool tips to listbox entries
    private static native void addItemWithTitle(Element element, String displayText, String value)/*-{
        var opt = $doc.createElement("OPTION");
        opt.title = value;
        opt.text = displayText;
        opt.value = value;
        element.options.add(opt);

    }-*/;

    private void updateFileList(String baseDir, boolean isUserFile, String[] filePaths) {
        String oldSelectedValue = contentListbox.getSelectedValue();
        // remove old entries
        if (isUserFile) {
            addFileEntries(userEntries, baseDir, false, filePaths);
        } else {
            addFileEntries(systemEntries, baseDir, true, filePaths);
        }
        contentListbox.clear();
        int selectedIndex = -1;
        int index = 0;
        for (Entry entry : userEntries) {
            addItemWithTitle(contentListbox.getElement(), entry.displayText, entry.path);
            if (oldSelectedValue != null && oldSelectedValue.equals(entry.path)) {
                selectedIndex = index;
            }
            index++;
        }
        for (Entry entry : systemEntries) {
            addItemWithTitle(contentListbox.getElement(), entry.displayText, entry.path);
            if (oldSelectedValue != null && oldSelectedValue.equals(entry.path)) {
                selectedIndex = index;
            }
            index++;
        }
        if (selectedIndex != -1) {
            contentListbox.setSelectedIndex(selectedIndex);
        }
        if (removeButton != null) {
            removeButton.setEnabled(userEntries.size() > 0 && selectedIndex != -1);
        }
        if (removeAllButton != null) {
            removeAllButton.setEnabled(userEntries.size() > 0);
        }
    }

    private static void addFileEntries(List<Entry> entries, String baseDir, boolean isSystem, String[] pathes) {
        entries.clear();
        for (String path : pathes) {

            int baseDirPos = path.lastIndexOf("/" + baseDir + "/");
            String filename;
            if (baseDirPos >= 0) {
                filename = path.substring(baseDirPos + baseDir.length() + 2);
            } else {
                filename = path.substring(path.lastIndexOf("/") + 1);
            }
            String displayText = filename;
            if (isSystem) {
                displayText = "(system) " + filename;
            }
            entries.add(new Entry(path, filename, displayText, isSystem));
        }
        entries.sort(Comparator.comparing(e -> e.displayText));
    }

    public String getSelectedFilePath() {
        int selectedIndex = contentListbox.getSelectedIndex();
        return selectedIndex >= 0 ? contentListbox.getValue(selectedIndex) : "";
    }

    public String getSelectedUserFilename() {
        int selectedIndex = contentListbox.getSelectedIndex();
        if (selectedIndex >= 0) {
            String path = contentListbox.getValue(selectedIndex);
            for (Entry entry : userEntries) {
                if (entry.path.equals(path)) {
                    return entry.filename;
                }
            }
        }
        return null;
    }

    public void setSelectedFilePath(String filePath) {
        for (int i = 0; i < contentListbox.getItemCount(); i++) {
            if (contentListbox.getValue(i).equals(filePath)) {
                contentListbox.setSelectedIndex(i);
                return;
            }
        }
        // TODO handle failure
    }

    private static class Entry {
        final String path;
        final String filename;
        final String displayText;
        final boolean isSystemFile;

        public Entry(String path, String filename, String displayText, boolean isSystemFile) {
            this.path = path;
            this.filename = filename;
            this.displayText = displayText;
            this.isSystemFile = isSystemFile;
        }
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
            updateUserFiles(true);
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
            final String recordSource = getSelectedUserFilename();
            if (recordSource == null) {
                Dialog.error("Remove File", "Cannot remove non-user entry.");
            } else {
                Dialog.ask("Remove File",
                           new HTML("The file '" + recordSource + "' will be permanently deleted.<br/>" +
                                    "Do you really want to continue?"),
                           new Runnable() {
                               @Override
                               public void run() {
                                   removeRecordSource(recordSource);
                               }
                           });
            }
        }
    }

    private class RemoveAllAction implements ClickHandler {

        @Override
        public void onClick(ClickEvent event) {
            if (!userEntries.isEmpty()) {
                String[] filePaths = new String[userEntries.size()];
                for (int i = 0; i < userEntries.size(); i++) {
                    filePaths[i] = userEntries.get(i).path;
                }
                Dialog.ask("Remove all Files",
                           new HTML("All files will be permanently deleted.<br/>" +
                                    "Do you really want to continue?"),
                           new Runnable() {
                               @Override
                               public void run() {
                                   removeRecordSources(filePaths);
                               }
                           });
            } else {
                Dialog.error("Remove all Files",
                             "No file to remove.");
            }
        }
    }

}
