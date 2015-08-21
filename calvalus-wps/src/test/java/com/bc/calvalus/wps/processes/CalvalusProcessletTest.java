package com.bc.calvalus.wps.processes;

import static org.mockito.Mockito.*;

import com.bc.calvalus.production.ProductionRequest;
import org.deegree.services.wps.ProcessletExecutionInfo;
import org.deegree.services.wps.ProcessletInputs;
import org.deegree.services.wps.ProcessletOutputs;
import org.junit.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hans on 23.07.2015.
 */
public class CalvalusProcessletTest {

    private ProcessletInputs mockProcessletInputs;
    private ProcessletOutputs mockProcessletOutputs;
    private ProcessletExecutionInfo mockProcessletExecutionInfo;

    @Before
    public void setUp() throws Exception {
//        mockProcessletInputs = getMockProcessletInputs();
        mockProcessletOutputs = mock(ProcessletOutputs.class);
        mockProcessletExecutionInfo = mock(ProcessletExecutionInfo.class);
    }

    @Ignore
    @Test
    public void testProcess() throws Exception {
        CalvalusProcesslet calvalusProcesslet = new CalvalusProcesslet();
        calvalusProcesslet.process(mockProcessletInputs, mockProcessletOutputs, mockProcessletExecutionInfo);

    }

    @Test
    public void canCreateProductionRequest() throws Exception {
//        Map<String, String> inputMap = calvalusDataInputs.convertInputToMap();
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put("productionName", "test1");
        inputMap.put("processorBundleName", "test2");
        ProductionRequest request = new ProductionRequest("Calvalus", "hans", inputMap);
        System.out.println("finished");
    }
}