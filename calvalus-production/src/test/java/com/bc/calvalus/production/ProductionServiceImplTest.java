package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.store.MemoryProductionStore;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class ProductionServiceImplTest {

    private ProductionServiceImpl productionServiceUnderTest;

    private TestInventoryService inventoryServiceMock;
    private TestProcessingService processingServiceMock;
    private TestStagingService stagingServiceMock;
    private MemoryProductionStore productionStoreMock;
    private TestProductionType productionTypeMock;

    @Before
    public void setUp() throws Exception {
        inventoryServiceMock = new TestInventoryService();
        processingServiceMock = new TestProcessingService();
        stagingServiceMock = new TestStagingService();
        productionTypeMock = new TestProductionType(processingServiceMock,
                                                    stagingServiceMock);
        productionStoreMock = new MemoryProductionStore();
        productionServiceUnderTest = new ProductionServiceImpl(inventoryServiceMock,
                                                               processingServiceMock,
                                                               stagingServiceMock,
                                                               productionStoreMock,
                                                               productionTypeMock);
    }

    @Test
    public void testOrderProduction() throws ProductionException {

        ProductionRequest request = new ProductionRequest("test", "ewa");
        ProductionResponse productionResponse = productionServiceUnderTest.orderProduction(request);
        assertNotNull(productionResponse);
        assertNotNull(productionResponse.getProduction());
        assertEquals("id_1", productionResponse.getProduction().getId());
        assertEquals("name_1", productionResponse.getProduction().getName());
        assertNotNull(productionResponse.getProduction().getJobIds());
        assertEquals(2, productionResponse.getProduction().getJobIds().length);
        assertEquals("job_1_1", productionResponse.getProduction().getJobIds()[0]);
        assertEquals("job_1_2", productionResponse.getProduction().getJobIds()[1]);
        assertNotNull(productionResponse.getProduction().getProductionRequest());
        assertEquals(request, productionResponse.getProduction().getProductionRequest());
        assertEquals("stagingPath_1", productionResponse.getProduction().getStagingPath());
    }

    @Test
    public void testOrderUnknownProductionType() {
        try {
            productionServiceUnderTest.orderProduction(new ProductionRequest("erase-hdfs", "devil"));
            fail("ProductionException expected, since 'erase-hdfs' is not a valid production type");
        } catch (ProductionException e) {
            // expected
        }
    }

    @Test
    public void testGetProductions() throws ProductionException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

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

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(3, productions.length);

        assertEquals(ProcessStatus.UNKNOWN, productions[0].getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, productions[1].getProcessingStatus());
        assertEquals(ProcessStatus.UNKNOWN, productions[2].getProcessingStatus());

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.RUNNING, 0.2f));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.RUNNING, 0.4f));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.SCHEDULED));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.RUNNING, 0.8f));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.3f), productions[0].getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.COMPLETED), productions[1].getProcessingStatus());
        assertEquals(new ProcessStatus(ProcessState.RUNNING, 0.4f), productions[2].getProcessingStatus());
    }


    @Test
    public void testDeleteProductions() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        productionServiceUnderTest.deleteProductions("id_2", "id_4");

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(5, productions.length);// cannot delete, because its status is not 'done'

        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.RUNNING));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);// now can delete id_2, because its status is not 'done'

        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.COMPLETED));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

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

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa"));

        processingServiceMock.setJobStatus("job_1_1", new ProcessStatus(ProcessState.SCHEDULED));
        processingServiceMock.setJobStatus("job_1_2", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_2_1", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_2_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_3_1", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_3_2", new ProcessStatus(ProcessState.COMPLETED));
        processingServiceMock.setJobStatus("job_4_1", new ProcessStatus(ProcessState.RUNNING));
        processingServiceMock.setJobStatus("job_4_2", new ProcessStatus(ProcessState.RUNNING));

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        Production[] productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);
        assertEquals(ProcessState.RUNNING, productions[0].getProcessingStatus().getState());
        assertEquals(ProcessState.COMPLETED, productions[1].getProcessingStatus().getState());
        assertEquals(ProcessState.RUNNING, productions[2].getProcessingStatus().getState());
        assertEquals(ProcessState.RUNNING, productions[3].getProcessingStatus().getState());

        productionServiceUnderTest.cancelProductions("id_1", "id_2", "id_4");

        // this would be called by the StatusObserver timer task
        productionServiceUnderTest.updateStatuses("ewa");

        productions = productionServiceUnderTest.getProductions(null);
        assertNotNull(productions);
        assertEquals(4, productions.length);
        assertEquals(ProcessState.CANCELLED, productions[0].getProcessingStatus().getState());
        assertEquals(ProcessState.COMPLETED, productions[1].getProcessingStatus().getState());
        assertEquals(ProcessState.RUNNING, productions[2].getProcessingStatus().getState());
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
    public void testStageProduction() throws ProductionException, IOException {

        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "false"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "false"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "true"));
        productionServiceUnderTest.orderProduction(new ProductionRequest("test", "ewa", "autoStaging", "true"));

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
        productionServiceUnderTest.updateStatuses("ewa");

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

    @Test
    public void testClose() throws Exception {
        assertEquals(false, stagingServiceMock.isClosed());
        assertEquals(false, processingServiceMock.isClosed());
        assertEquals(false, productionStoreMock.isClosed());
        productionServiceUnderTest.close();
        assertEquals(true, stagingServiceMock.isClosed());
        assertEquals(true, processingServiceMock.isClosed());
        assertEquals(true, productionStoreMock.isClosed());
    }

    @Test
    public void testRegularExpression() throws Exception {
        Pattern pattern = Pattern.compile("(.*)@(.*):(.*)");
        Matcher matcher = pattern.matcher("freshmon@freshmon-csw:/data/postprocessing/entries");
        assertEquals(3, matcher.groupCount());
        assertEquals(true, matcher.find());
        assertEquals("freshmon@freshmon-csw:/data/postprocessing/entries", matcher.group(0));
        assertEquals("freshmon", matcher.group(1));
        assertEquals("freshmon-csw", matcher.group(2));
        assertEquals("/data/postprocessing/entries", matcher.group(3));

    }
}