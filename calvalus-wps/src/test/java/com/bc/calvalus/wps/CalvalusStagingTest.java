package com.bc.calvalus.wps;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.calvalusfacade.CalvalusStaging;
import org.junit.*;
import org.mockito.ArgumentCaptor;

/**
 * Created by hans on 10/08/2015.
 */
public class CalvalusStagingTest {

    private static final String DUMMY_ID = "dummy-id";
    private static final String DUMMY_USER_NAME = "dummy-userName";

    private ProductionService mockProductionService;
    private Production mockProduction;
    private ProductionRequest mockProductionRequest;
    private ProcessStatus mockProcessStatus;

    /**
     * Class under test.
     */
    private CalvalusStaging calvalusStaging;

    @Before
    public void setUp() throws Exception {
        mockProductionService = mock(ProductionService.class);
        mockProduction = mock(Production.class);
        mockProductionRequest = mock(ProductionRequest.class);
        mockProcessStatus = mock(ProcessStatus.class);

        when(mockProduction.getId()).thenReturn(DUMMY_ID);
        doNothing().when(mockProductionService).stageProductions(anyString());
        when(mockProductionRequest.getUserName()).thenReturn(DUMMY_USER_NAME);
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
    }

    @Test
    public void testStageProductionInitiallyDone() throws Exception {
        when(mockProcessStatus.isDone()).thenReturn(true);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.COMPLETED);
        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatus);

        calvalusStaging = new CalvalusStaging();
        calvalusStaging.stageProduction(mockProductionService, mockProduction);

        verify(mockProductionService).stageProductions(DUMMY_ID);
        verify(mockProductionService, times(0)).updateStatuses(anyString());
        verify(mockProcessStatus, times(0)).getMessage();
    }

    @Test
    public void testStageProductionInitiallyNotDone() throws Exception {
        ArgumentCaptor<String> userName = ArgumentCaptor.forClass(String.class);

        when(mockProcessStatus.isDone()).thenReturn(false, false, false, true);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.COMPLETED);
        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatus);

        calvalusStaging = new CalvalusStaging();
        calvalusStaging.stageProduction(mockProductionService, mockProduction);

        verify(mockProductionService).stageProductions(DUMMY_ID);
        verify(mockProductionService, times(3)).updateStatuses(userName.capture());

        assertThat(userName.getValue(), equalTo("dummy-userName"));
    }

    @Test
    public void testStageProductionDoneButNotCompleted() throws Exception {
        when(mockProcessStatus.isDone()).thenReturn(true);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.RUNNING);
        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatus);

        calvalusStaging = new CalvalusStaging();
        calvalusStaging.stageProduction(mockProductionService, mockProduction);

        verify(mockProductionService).stageProductions(DUMMY_ID);
        verify(mockProductionService, times(0)).updateStatuses(anyString());
        verify(mockProcessStatus, times(1)).getMessage();
    }
}