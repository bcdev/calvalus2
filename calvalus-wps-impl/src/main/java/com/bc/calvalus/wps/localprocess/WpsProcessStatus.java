package com.bc.calvalus.wps.localprocess;

import java.util.Date;
import java.util.List;

/**
 * @author hans
 */
public interface WpsProcessStatus {

    String getJobId();

    String getState();

    float getProgress();

    String getMessage();

    List<String> getResultUrls();

    Date getStopTime();

    boolean isDone();

}
