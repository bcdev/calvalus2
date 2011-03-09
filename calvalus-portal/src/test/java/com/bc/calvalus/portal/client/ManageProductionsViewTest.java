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
        assertEquals("Cancel", getAction(WAITING, UNKNOWN));
        assertEquals("Cancel", getAction(IN_PROGRESS, UNKNOWN));
        assertEquals("Restart", getAction(COMPLETED, UNKNOWN));
        assertEquals("Restart", getAction(CANCELLED, UNKNOWN));
        assertEquals("Restart", getAction(ERROR, UNKNOWN));
        assertEquals("Cancel", getAction(WAITING, WAITING));
        assertEquals("Cancel", getAction(IN_PROGRESS, WAITING));
        assertEquals("Cancel", getAction(COMPLETED, WAITING));
        assertEquals("Restart", getAction(CANCELLED, WAITING)); // todo - weird status, why is staging WAITING? (nf)
        assertEquals("Restart", getAction(ERROR, WAITING));     // todo - weird status, why is staging WAITING? (nf)
        assertEquals("Cancel", getAction(COMPLETED, IN_PROGRESS));
        assertEquals("Restart", getAction(COMPLETED, COMPLETED));
        assertEquals("Restart", getAction(COMPLETED, CANCELLED));
        assertEquals("Restart", getAction(COMPLETED, ERROR));
    }

    public void testGetResult() {
        assertEquals(null, getResult(UNKNOWN, UNKNOWN));
        assertEquals(null, getResult(WAITING, UNKNOWN));
        assertEquals(null, getResult(IN_PROGRESS, UNKNOWN));
        assertEquals("Stage", getResult(COMPLETED, UNKNOWN));
        assertEquals(null, getResult(CANCELLED, UNKNOWN));
        assertEquals(null, getResult(ERROR, UNKNOWN));
        assertEquals(null, getResult(WAITING, WAITING));
        assertEquals(null, getResult(IN_PROGRESS, WAITING));
        assertEquals(null, getResult(COMPLETED, WAITING));
        assertEquals(null, getResult(CANCELLED, WAITING));
        assertEquals(null, getResult(ERROR, WAITING));
        assertEquals(null, getResult(COMPLETED, IN_PROGRESS));
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
