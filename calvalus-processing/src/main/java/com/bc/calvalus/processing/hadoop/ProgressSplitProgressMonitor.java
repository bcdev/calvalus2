/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.hadoop;

import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * A progress monitor (ab-)uses the {@link ProgressSplit} to reports it progress to hadoop.
 */
public class ProgressSplitProgressMonitor implements ProgressMonitor, Progressable {
    private float totalWork;
    private final MapContext mapContext;
    private final ProgressSplit progressSplit;
    private float work;

    public ProgressSplitProgressMonitor(MapContext mapContext) {
        this.mapContext = mapContext;
        InputSplit inputSplit = mapContext.getInputSplit();
        if (inputSplit instanceof ProgressSplit) {
            progressSplit = (ProgressSplit) inputSplit;
        } else {
            this.progressSplit = null;
        }
    }

    @Override
    public void beginTask(String taskName, int totalWork) {
        setTaskName(taskName);
        this.totalWork = totalWork;
    }

    @Override
    public void worked(int delta) {
        internalWorked(delta);
    }

    @Override
    public void progress() {
        mapContext.progress();
    }

    @Override
    public void done() {
        mapContext.setStatus("");
        mapContext.progress();
    }

    @Override
    public void internalWorked(double deltaWork) {
        if (progressSplit !=null) {
            work += deltaWork;
            progressSplit.setProgress(Math.min(1.0f, work / totalWork));
            try {
                // trigger progress propagation (yes, that's weird but we don't use true input formats
                // that are responsible for read progress)
                mapContext.nextKeyValue();
            } catch (IOException e) {
                // ignore
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    @Override
    public boolean isCanceled() {
        return false;
    }

    @Override
    public void setCanceled(boolean canceled) {
    }

    @Override
    public void setTaskName(String taskName) {
        mapContext.setStatus(taskName);
    }

    @Override
    public void setSubTaskName(String subTaskName) {
        mapContext.setStatus(subTaskName);
    }
}
