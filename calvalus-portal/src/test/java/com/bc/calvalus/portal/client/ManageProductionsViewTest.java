package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.DtoProcessState;
import com.bc.calvalus.portal.shared.DtoProcessStatus;
import com.bc.calvalus.portal.shared.DtoProduction;
import junit.framework.TestCase;

import static com.bc.calvalus.portal.shared.DtoProcessState.*;

public class ManageProductionsViewTest extends TestCase {

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

    public void testGetStageType() {
        String[] additionalStagingPaths = null;
        assertEquals(StageType.NO_STAGING, getStageType(UNKNOWN, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(SCHEDULED, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(RUNNING, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.STAGE, getStageType(COMPLETED, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(CANCELLED, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(ERROR, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(COMPLETED, SCHEDULED, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(COMPLETED, RUNNING, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(COMPLETED, COMPLETED, additionalStagingPaths));
        assertEquals(StageType.STAGE, getStageType(COMPLETED, CANCELLED, additionalStagingPaths));
        assertEquals(StageType.STAGE, getStageType(COMPLETED, ERROR, additionalStagingPaths));

    }

    public void testGetMultiStageType() {
        String[] additionalStagingPaths = new String[]{"one/path", "second/path"};
        assertEquals(StageType.MULTI_STAGE, getStageType(COMPLETED, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.MULTI_STAGE, getStageType(COMPLETED, CANCELLED, additionalStagingPaths));
        assertEquals(StageType.MULTI_STAGE, getStageType(COMPLETED, ERROR, additionalStagingPaths));
        assertEquals(StageType.MULTI_STAGE, getStageType(COMPLETED, SCHEDULED, additionalStagingPaths));
        assertEquals(StageType.MULTI_STAGE, getStageType(COMPLETED, RUNNING, additionalStagingPaths));
        assertEquals(StageType.MULTI_STAGE, getStageType(COMPLETED, COMPLETED, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(UNKNOWN, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(SCHEDULED, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(RUNNING, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(CANCELLED, UNKNOWN, additionalStagingPaths));
        assertEquals(StageType.NO_STAGING, getStageType(ERROR, UNKNOWN, additionalStagingPaths));
    }

    public void testGetAutoStageType() {
        assertEquals(StageType.NO_STAGING, getResultAutoStaging(UNKNOWN, UNKNOWN));
        assertEquals(StageType.NO_STAGING, getResultAutoStaging(SCHEDULED, UNKNOWN));
        assertEquals(StageType.NO_STAGING, getResultAutoStaging(RUNNING, UNKNOWN));
        assertEquals(StageType.AUTO_STAGING, getResultAutoStaging(COMPLETED, UNKNOWN));
        assertEquals(StageType.NO_STAGING, getResultAutoStaging(CANCELLED, UNKNOWN));
        assertEquals(StageType.NO_STAGING, getResultAutoStaging(ERROR, UNKNOWN));
        assertEquals(StageType.NO_STAGING, getResultAutoStaging(COMPLETED, SCHEDULED));
        assertEquals(StageType.NO_STAGING, getResultAutoStaging(COMPLETED, RUNNING));
        assertEquals(StageType.STAGE, getResultAutoStaging(COMPLETED, CANCELLED));
        assertEquals(StageType.STAGE, getResultAutoStaging(COMPLETED, ERROR));
    }

    public void testGetDownloadText() {
        assertEquals(null, getDownloadText(COMPLETED, RUNNING));
        assertEquals(null, getDownloadText(COMPLETED, CANCELLED));
        assertEquals("Download", getDownloadText(COMPLETED, COMPLETED));
    }

    public void testGetTimeText() {
        assertEquals("", ManageProductionsView.getTimeText(0));
        assertEquals("0:00:01", ManageProductionsView.getTimeText(1));
        assertEquals("0:00:10", ManageProductionsView.getTimeText(10));
        assertEquals("0:01:40", ManageProductionsView.getTimeText(100));
        assertEquals("1:00:00", ManageProductionsView.getTimeText(3600));
        assertEquals("100:00:00", ManageProductionsView.getTimeText(360000));
    }

    public void testGetProgressText() throws Exception {
        assertEquals("0.0%", ManageProductionsView.getProgressText(0.0F));
        assertEquals("50.0%", ManageProductionsView.getProgressText(0.5F));
        assertEquals("57.5%", ManageProductionsView.getProgressText(0.5752F));
        assertEquals("57.5%", ManageProductionsView.getProgressText(0.5758F));  // no rounding!
        assertEquals("99.9%", ManageProductionsView.getProgressText(0.9999F));
        assertEquals("100.0%", ManageProductionsView.getProgressText(1F));
    }

    private String getAction(DtoProcessState productionState, DtoProcessState stagingState) {
        return ManageProductionsView.getAction(createProduction(productionState, stagingState, false,
                                                                null));
    }

    private StageType getStageType(DtoProcessState productionState,
                                   DtoProcessState stagingState, String[] additionalStagingPaths) {
        DtoProduction production = createProduction(productionState, stagingState, false, additionalStagingPaths);
        return ManageProductionsView.getStageType(production);
    }

    private String getDownloadText(DtoProcessState productionState, DtoProcessState stagingState) {
        return ManageProductionsView.getDownloadText(createProduction(productionState, stagingState, false,
                                                                      null));
    }

    private StageType getResultAutoStaging(DtoProcessState productionState,
                                           DtoProcessState stagingState) {
        return ManageProductionsView.getStageType(createProduction(productionState, stagingState, true,
                                                                   null));
    }

    private DtoProduction createProduction(DtoProcessState productionState, DtoProcessState stagingState,
                                           boolean autoStaging, String[] additionalStagingPaths) {
        return new DtoProduction("id", "name", "user", "outputPath", "stagingPath", additionalStagingPaths, autoStaging,
                                 new DtoProcessStatus(productionState), new DtoProcessStatus(stagingState));
    }


}
