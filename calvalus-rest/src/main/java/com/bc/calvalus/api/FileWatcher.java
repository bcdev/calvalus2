package com.bc.calvalus.api;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class FileWatcher {

    private final List<FileWatchListener> listeners;
    private final FileTimeContainer fileContainer;

    FileWatcher(File file) {
        listeners = new ArrayList<>();
        fileContainer = new FileTimeContainer();
        fileContainer.file = file;
        fileContainer.lastModified = file.lastModified();
    }

    void addListener(FileWatchListener listener) {
        if (listener != null && !listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    void start() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                checkForModified();
            }
        }, 0, 10000);
    }

    private void checkForModified() {
        if (hasFileChanged()) {
            for (FileWatchListener listener : listeners) {
                listener.fileChanged();
            }
        }
    }

    private boolean hasFileChanged() {
        final File file = fileContainer.file;
        final long lastModified = file.lastModified();
        if (fileContainer.lastModified != lastModified) {
            fileContainer.lastModified = lastModified;
            return true;
        }
        return false;
    }


    public interface FileWatchListener {
        void fileChanged();
    }

    private static class FileTimeContainer {
        long lastModified;
        File file;
    }

}
