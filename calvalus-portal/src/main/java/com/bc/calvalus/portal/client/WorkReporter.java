package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.WorkStatus;

/**
 * Something that has a reportable progress.
 *
 * @author Norman
 */
public interface WorkReporter {
    WorkStatus getWorkStatus();
}
