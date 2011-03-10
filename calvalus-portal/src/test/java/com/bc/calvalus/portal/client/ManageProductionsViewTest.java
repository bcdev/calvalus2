package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.GsProcessState;
import com.bc.calvalus.portal.shared.GsProcessStatus;
import com.bc.calvalus.portal.shared.GsProduction;
import com.google.gwt.junit.client.GWTTestCase;

import static com.bc.calvalus.portal.shared.GsProcessState.*;

public class ManageProductionsViewTest extends GWTTestCase {
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testGetAction() {
        assertEquals(null, getAction(UNKNOWN, UNKNOWN));
        assertEquals("Cancel", getAction(SCHEDULED, UNKNOWN));
        assertEquals("Cancel", getAction(RUNNING, UNKNOWN));
        assertEquals("Restart", getAction(COMPLETED, UNKNOWN));
        assertEquals("Restart", getAction(CANCELLED, UNKNOWN));
        assertEquals("Restart", getAction(ERROR, UNKNOWN));
        assertEquals("Cancel", getAction(SCHEDULED, SCHEDULED));
        assertEquals("Cancel", getAction(RUNNING, SCHEDULED));
        assertEquals("Cancel", getAction(COMPLETED, SCHEDULED));
        assertEquals("Restart", getAction(CANCELLED, SCHEDULED)); // todo - weird status, why is staging WAITING? (nf)
        assertEquals("Restart", getAction(ERROR, SCHEDULED));     // todo - weird status, why is staging WAITING? (nf)
        assertEquals("Cancel", getAction(COMPLETED, RUNNING));
        assertEquals("Restart", getAction(COMPLETED, COMPLETED));
        assertEquals("Restart", getAction(COMPLETED, CANCELLED));
        assertEquals("Restart", getAction(COMPLETED, ERROR));
    }

    public void testGetResult() {
        assertEquals(null, getResult(UNKNOWN, UNKNOWN));
        assertEquals(null, getResult(SCHEDULED, UNKNOWN));
        assertEquals(null, getResult(RUNNING, UNKNOWN));
        assertEquals("Stage", getResult(COMPLETED, UNKNOWN));
        assertEquals(null, getResult(CANCELLED, UNKNOWN));
        assertEquals(null, getResult(ERROR, UNKNOWN));
        assertEquals(null, getResult(SCHEDULED, SCHEDULED));
        assertEquals(null, getResult(RUNNING, SCHEDULED));
        assertEquals(null, getResult(COMPLETED, SCHEDULED));
        assertEquals(null, getResult(CANCELLED, SCHEDULED));
        assertEquals(null, getResult(ERROR, SCHEDULED));
        assertEquals(null, getResult(COMPLETED, RUNNING));
        assertEquals("Download", getResult(COMPLETED, COMPLETED));
        assertEquals("Stage", getResult(COMPLETED, CANCELLED));
        assertEquals("Stage", getResult(COMPLETED, ERROR));
    }

    private String getAction(GsProcessState productionState, GsProcessState stagingState) {
        return ManageProductionsView.getAction(new GsProduction("id", "name", "user", "outputUrl",
                                                                    new GsProcessStatus(productionState),
                                                                    new GsProcessStatus(stagingState)));
    }

    private String getResult(GsProcessState productionState, GsProcessState stagingState) {
        return ManageProductionsView.getResult(new GsProduction("id", "name", "user", "outputUrl",
                                                                    new GsProcessStatus(productionState),
                                                                    new GsProcessStatus(stagingState)));
    }
}
