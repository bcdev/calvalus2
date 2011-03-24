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
        assertEquals("Cancel", getAction(COMPLETED, SCHEDULED));
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
        assertEquals(null, getResult(COMPLETED, SCHEDULED));
        assertEquals(null, getResult(COMPLETED, RUNNING));
        assertEquals("Download", getResult(COMPLETED, COMPLETED));
        assertEquals("Stage", getResult(COMPLETED, CANCELLED));
        assertEquals("Stage", getResult(COMPLETED, ERROR));
    }

    public void testGetResultAutoStaging() {
        assertEquals(null, getResultAutoStaging(UNKNOWN, UNKNOWN));
        assertEquals(null, getResultAutoStaging(SCHEDULED, UNKNOWN));
        assertEquals(null, getResultAutoStaging(RUNNING, UNKNOWN));
        assertEquals("#Auto-staging", getResultAutoStaging(COMPLETED, UNKNOWN));
        assertEquals(null, getResultAutoStaging(CANCELLED, UNKNOWN));
        assertEquals(null, getResultAutoStaging(ERROR, UNKNOWN));
        assertEquals(null, getResultAutoStaging(COMPLETED, SCHEDULED));
        assertEquals(null, getResultAutoStaging(COMPLETED, RUNNING));
        assertEquals("Download", getResultAutoStaging(COMPLETED, COMPLETED));
        assertEquals("Stage", getResultAutoStaging(COMPLETED, CANCELLED));
        assertEquals("Stage", getResultAutoStaging(COMPLETED, ERROR));
    }

    public void testGetTimeText() {
        assertEquals("", ManageProductionsView.getTimeText(0));
        assertEquals("0:00:01", ManageProductionsView.getTimeText(1));
        assertEquals("0:00:10", ManageProductionsView.getTimeText(10));
        assertEquals("0:01:40", ManageProductionsView.getTimeText(100));
        assertEquals("1:00:00", ManageProductionsView.getTimeText(3600));
        assertEquals("100:00:00", ManageProductionsView.getTimeText(360000));
    }

    private String getAction(GsProcessState productionState, GsProcessState stagingState) {
        return ManageProductionsView.getAction(createProduction(productionState, stagingState, false));
    }

    private String getResult(GsProcessState productionState, GsProcessState stagingState) {
        return ManageProductionsView.getResult(createProduction(productionState, stagingState, false));
    }

    private String getResultAutoStaging(GsProcessState productionState, GsProcessState stagingState) {
        return ManageProductionsView.getResult(createProduction(productionState, stagingState, true));
    }

    private GsProduction createProduction(GsProcessState productionState, GsProcessState stagingState, boolean autoStaging) {
        return new GsProduction("id", "name", "user", "outputUrl", autoStaging,
                                                                new GsProcessStatus(productionState),
                                                                new GsProcessStatus(stagingState));
    }
}
