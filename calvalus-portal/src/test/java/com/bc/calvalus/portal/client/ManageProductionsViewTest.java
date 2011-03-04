package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.PortalProduction;
import com.bc.calvalus.portal.shared.PortalProductionState;
import com.bc.calvalus.portal.shared.PortalProductionStatus;
import com.google.gwt.junit.client.GWTTestCase;

import static com.bc.calvalus.portal.shared.PortalProductionState.*;

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
        assertEquals("Cancel", getAction(CANCELLED, WAITING));
        assertEquals("Cancel", getAction(ERROR, WAITING));
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

    private String getAction(PortalProductionState productionState, PortalProductionState stagingState) {
        return ManageProductionsView.getAction(new PortalProduction("id", "name", "outputUrl",
                                                                    new PortalProductionStatus(productionState),
                                                                    new PortalProductionStatus(stagingState)));
    }

    private String getResult(PortalProductionState productionState, PortalProductionState stagingState) {
        return ManageProductionsView.getResult(new PortalProduction("id", "name", "outputUrl",
                                                                    new PortalProductionStatus(productionState),
                                                                    new PortalProductionStatus(stagingState)));
    }
}
