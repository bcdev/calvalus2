package com.bc.calvalus.wps.calvalusfacade;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import org.junit.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusWpsProcessStatusTest {

    private static final String JOB_ID = "job-01";
    private static final String ERROR_MESSAGE = "error message";
    private static final float PROGRESS_VALUE = 0.85f;
    private static final Date DATE = new Date(1451606400000L);

    private Production mockProduction;
    private ProcessStatus mockProcessStatus;
    private List<String> mockResultUrls;

    private CalvalusWpsProcessStatus status;

    @Before
    public void setUp() throws Exception {
        mockProduction = mock(Production.class);
        mockProcessStatus = mock(ProcessStatus.class);
        WorkflowItem mockWorkflow = mock(WorkflowItem.class);
        mockResultUrls = new ArrayList<>();

        when(mockProduction.getId()).thenReturn(JOB_ID);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.COMPLETED);
        when(mockProcessStatus.getProgress()).thenReturn(PROGRESS_VALUE);
        when(mockProcessStatus.getMessage()).thenReturn(ERROR_MESSAGE);
        when(mockProduction.getProcessingStatus()).thenReturn(mockProcessStatus);
        when(mockWorkflow.getStopTime()).thenReturn(DATE);
        when(mockProduction.getWorkflow()).thenReturn(mockWorkflow);

        status = new CalvalusWpsProcessStatus(mockProduction, mockResultUrls);
    }

    @Test
    public void canGetJobId() throws Exception {
        assertThat(status.getJobId(), equalTo(JOB_ID));
    }

    @Test
    public void canGetState() throws Exception {
        assertThat(status.getState(), equalTo(ProcessState.COMPLETED.toString()));
    }

    @Test
    public void canGetProgress() throws Exception {
        assertThat(status.getProgress(), equalTo(0.85f * 100));
    }

    @Test
    public void canGetMessage() throws Exception {
        assertThat(status.getMessage(), equalTo(ERROR_MESSAGE));
    }

    @Test
    public void canGetResultUrls() throws Exception {
        assertThat(status.getResultUrls(), equalTo(mockResultUrls));
    }

    @Test
    public void canGetStopTime() throws Exception {
        assertThat(status.getStopTime(), equalTo(DATE));
    }

    @Test
    public void canReturnTrueWhenProcessIsDone() throws Exception {
        assertThat(status.isDone(), equalTo(true));
    }

    @Test
    public void canReturnFalseWhenProcessIsNotDone() throws Exception {
        when(mockProcessStatus.getState()).thenReturn(ProcessState.RUNNING);
        when(mockProcessStatus.getProgress()).thenReturn(PROGRESS_VALUE);
        when(mockProcessStatus.getMessage()).thenReturn(ERROR_MESSAGE);
        when(mockProduction.getProcessingStatus()).thenReturn(mockProcessStatus);

        status = new CalvalusWpsProcessStatus(mockProduction, mockResultUrls);

        assertThat(status.isDone(), equalTo(false));
    }
}