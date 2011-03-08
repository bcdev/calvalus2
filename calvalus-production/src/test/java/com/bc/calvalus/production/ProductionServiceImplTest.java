package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ProductionServiceImplTest {
    private TestProcessingService processingServiceMock;
    private ProductionServiceImpl productionServiceUnderTest;
    private TestProductionStore productionStoreMock;
    private TestStagingService stagingServiceMock;
    private TestProductionType productionTypeMock;


    @Before
    public void setUp() throws Exception {
        processingServiceMock = new TestProcessingService();
        stagingServiceMock = new TestStagingService();
        productionTypeMock = new TestProductionType();
        productionStoreMock = new TestProductionStore();
        productionServiceUnderTest = new ProductionServiceImpl(processingServiceMock, stagingServiceMock, productionStoreMock,
                                                               productionTypeMock);
    }

    @Test
    public void testOrderProduction() throws Exception {

        ProductionRequest request = new ProductionRequest("test");
        ProductionResponse productionResponse = productionServiceUnderTest.orderProduction(request);
        assertNotNull(productionResponse);
        assertNotNull(productionResponse.getProduction());
        assertEquals("id_1", productionResponse.getProduction().getId());
        assertEquals("name_1", productionResponse.getProduction().getName());
        assertEquals("user_1", productionResponse.getProduction().getUser());
        assertNotNull(productionResponse.getProduction().getJobIds());
        assertEquals(2, productionResponse.getProduction().getJobIds().length);
        assertEquals("job_1_1", productionResponse.getProduction().getJobIds()[0]);
        assertEquals("job_1_2", productionResponse.getProduction().getJobIds()[1]);
        assertNotNull(productionResponse.getProduction().getProductionRequest());
        assertEquals(request, productionResponse.getProduction().getProductionRequest());
    }

    @Test
    public void testOrderUnknownProductionType() throws Exception {
        try {
            productionServiceUnderTest.orderProduction(new ProductionRequest("erase"));
            fail("ProductionException expected, since 'erase' is not a valid production type");
        } catch (ProductionException e) {
            // expected
        }
    }

    @Test
    public void testGetProductions() throws ProductionException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);
        assertEquals("id_1", productions[0].getId());
        assertEquals("id_2", productions[1].getId());
        assertEquals("id_3", productions[2].getId());

        // Make sure data store is used
        assertSame(productions[0], productionStoreMock.getProduction("id_1"));
        assertSame(productions[1], productionStoreMock.getProduction("id_2"));
        assertSame(productions[2], productionStoreMock.getProduction("id_3"));
        assertNull(productionStoreMock.getProduction("id_x"));
    }


    @Test
    public void testGetProductionStatusPropagation() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);

        assertEquals(ProcessStatus.UNKNOWN, productions[0].getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, productions[1].getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, productions[2].getProcessingStatus());

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.IN_PROGRESS, 0.2f));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.IN_PROGRESS, 0.4f));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.WAITING));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.IN_PROGRESS, 0.8f));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateProductions();

        assertEquals(new ProcessStatus(ProcessState.IN_PROGRESS, 0.3f), productions[0].getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), productions[1].getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.IN_PROGRESS, 0.4f), productions[2].getProcessingStatus());
    }


    @Test
    public void testDeleteProductions() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));

        productionServiceUnderTest.deleteProductions("id_2", "id_4");

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(5, productions.length);// cannot delete, because its status is not 'done'

        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.IN_PROGRESS));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.IN_PROGRESS));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateProductions();

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);// now can delete id_2, because its status is not 'done'

        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.COMPLETED));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateProductions();

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);// now can delete id_4, because its status is not 'done'
    }

    @Test
    public void testDeleteUnknownProduction() {
        try {
            productionServiceUnderTest.deleteProductions("id_45");
            fail("ProductionException expected, because we don't have production 'id_45'");
        } catch (ProductionException e) {
            // ok
        }
    }

    @Test
    public void testCancelProductions() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.WAITING));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.IN_PROGRESS));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_1", new ProcessStatus(ProcessState.IN_PROGRESS));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.IN_PROGRESS));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.IN_PROGRESS));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateProductions();

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);
        assertEquals(ProcessState.IN_PROGRESS, productions[0].getProcessingStatus().getState());
        assertEquals(ProcessState.COMPLETED, productions[1].getProcessingStatus().getState());
        assertEquals(ProcessState.IN_PROGRESS, productions[2].getProcessingStatus().getState());
        assertEquals(ProcessState.IN_PROGRESS, productions[3].getProcessingStatus().getState());

        productionServiceUnderTest.cancelProductions("id_1", "id_2", "id_4");

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateProductions();

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);
        assertEquals(ProcessState.CANCELLED, productions[0].getProcessingStatus().getState());
        assertEquals(ProcessState.COMPLETED, productions[1].getProcessingStatus().getState());
        assertEquals(ProcessState.IN_PROGRESS, productions[2].getProcessingStatus().getState());
        assertEquals(ProcessState.CANCELLED, productions[3].getProcessingStatus().getState());
    }

    @Test
    public void testCancelUnknownProduction() {
        try {
            productionServiceUnderTest.cancelProductions("id_25");
            fail("ProductionException expected, because we don't have production 'id_25'");
        } catch (ProductionException e) {
            // ok
        }
    }

    @Test
    public void testStaging() throws ProductionException, IOException {

        productionTypeMock.setOutputStaging(false); // the following 2 productions will NOT use auto-staging
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionTypeMock.setOutputStaging(true); // the following 2 productions will use auto-staging once they are done
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test"));

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.COMPLETED));

        assertTrue(stagingServiceMock.getStagings().isEmpty());

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateProductions();

        assertEquals(2, stagingServiceMock.getStagings().size());

        productionServiceUnderTest.stageProductions("id_1", "id_2");

        assertEquals(4, stagingServiceMock.getStagings().size());
    }

    @Test
    public void testStageUnknownProduction() {
        try {
            productionServiceUnderTest.stageProductions("id_98");
            fail("ProductionException expected, because we don't have production 'id_98'");
        } catch (ProductionException e) {
            // ok
        }
    }
}