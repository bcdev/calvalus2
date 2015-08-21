package com.bc.calvalus.wps;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.CalvalusProduction;
import org.junit.*;

/**
 * Created by hans on 11/08/2015.
 */
public class CalvalusProductionTest {

    private ProductionService mockProductionService;
    private ProductionRequest mockProductionRequest;
    private ProductionResponse mockProductionResponse;
    private ProcessStatus mockProcessStatus;
    private WorkflowItem mockWorkflowItem;

    /**
     * Class under test.
     */
    private CalvalusProduction calvalusProduction;

    @Before
    public void setUp() throws Exception {
        mockProductionService = mock(ProductionService.class);
        mockProductionRequest = mock(ProductionRequest.class);
        mockProductionResponse = mock(ProductionResponse.class);
        mockProcessStatus = mock(ProcessStatus.class);
        mockWorkflowItem = mock(WorkflowItem.class);
    }

    @Test
    public void testOrderProductionInitiallyDone() throws Exception {
        Production mockProduction = mock(Production.class);
        when(mockProduction.getWorkflow()).thenReturn(mockWorkflowItem);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.COMPLETED);
        when(mockProduction.getProcessingStatus()).thenReturn(mockProcessStatus);
        when(mockProductionRequest.getUserName()).thenReturn("dummy-userName");
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockProductionResponse.getProduction()).thenReturn(mockProduction);
        when(mockProductionService.orderProduction(mockProductionRequest)).thenReturn(mockProductionResponse);

        calvalusProduction = new CalvalusProduction();
        Production expectedProduction = calvalusProduction.orderProduction(mockProductionService, mockProductionRequest);

        assertThat(expectedProduction, equalTo(mockProduction));
        verify(mockProductionService, times(0)).updateStatuses(anyString());
    }

    @Test
    public void testOrderProductionInitiallyNotDone() throws Exception {
        Production mockProduction = mock(Production.class);
        when(mockProduction.getWorkflow()).thenReturn(mockWorkflowItem);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.SCHEDULED, ProcessState.RUNNING, ProcessState.COMPLETED);
        when(mockProduction.getProcessingStatus()).thenReturn(mockProcessStatus);
        when(mockProductionRequest.getUserName()).thenReturn("dummy-userName");
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockProductionResponse.getProduction()).thenReturn(mockProduction);
        when(mockProductionService.orderProduction(mockProductionRequest)).thenReturn(mockProductionResponse);

        calvalusProduction = new CalvalusProduction();
        Production expectedProduction = calvalusProduction.orderProduction(mockProductionService, mockProductionRequest);

        assertThat(expectedProduction, equalTo(mockProduction));
        verify(mockProductionService, times(1)).updateStatuses(anyString());
        verify(mockProduction).getStagingPath();
    }

    @Test
    public void testOrderProductionDoneButNotCompleted() throws Exception {
        Production mockProduction = mock(Production.class);
        when(mockProduction.getWorkflow()).thenReturn(mockWorkflowItem);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.SCHEDULED, ProcessState.RUNNING, ProcessState.ERROR, ProcessState.ERROR);
        when(mockProduction.getProcessingStatus()).thenReturn(mockProcessStatus);
        when(mockProductionRequest.getUserName()).thenReturn("dummy-userName");
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockProductionResponse.getProduction()).thenReturn(mockProduction);
        when(mockProductionService.orderProduction(mockProductionRequest)).thenReturn(mockProductionResponse);

        calvalusProduction = new CalvalusProduction();
        Production expectedProduction = calvalusProduction.orderProduction(mockProductionService, mockProductionRequest);

        assertThat(expectedProduction, equalTo(mockProduction));
        verify(mockProductionService, times(1)).updateStatuses(anyString());
        verify(mockProduction, times(0)).getStagingPath();
        verify(mockProcessStatus, times(2)).getMessage();
    }
}