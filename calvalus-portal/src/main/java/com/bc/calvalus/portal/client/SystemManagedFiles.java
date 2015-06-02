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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages a list of files that can be uploaded and remove by the user.
 */
class SystemManagedFiles {

    private final BackendServiceAsync backendService;
    private final ListBox contentListbox;
    private final String baseDir;
    private final String what;

    SystemManagedFiles(BackendServiceAsync backendService, ListBox contentListbox, String baseDir, String what) {
        this.backendService = backendService;
        this.contentListbox = contentListbox;
        this.baseDir = baseDir;
        this.what = what;
    }


    public void updateList() {
        backendService.listSystemFiles(baseDir, new AsyncCallback<String[]>() {
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

    private void setItems(String[] filePaths) {
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

}
