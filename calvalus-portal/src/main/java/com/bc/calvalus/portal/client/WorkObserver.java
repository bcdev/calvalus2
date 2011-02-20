package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.WorkStatus;

/**
 * An observer for work.
 *
 * @author Norman
 */
public interface WorkObserver {
    void workStarted(WorkStatus workStatus);

    void workProgressing(WorkStatus workStatus);

    void workStopped(WorkStatus workStatus);
}
