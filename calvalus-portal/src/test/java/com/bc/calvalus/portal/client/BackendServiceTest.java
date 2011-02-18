package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.BackendService;
import com.bc.calvalus.portal.shared.BackendServiceAsync;
import com.bc.calvalus.portal.shared.PortalProductionRequest;
import com.bc.calvalus.portal.shared.PortalProductionResponse;
import com.google.gwt.core.client.GWT;
import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.ServiceDefTarget;

public class BackendServiceTest extends GWTTestCase {

    /**
     * Must refer to a valid module that sources this class.
     */
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    /**
     * Tests the ProcessingRequestVerifier.
     */
    public void testFieldVerifier() {
        assertFalse(PortalProductionRequest.isValid(new PortalProductionRequest(null, null, null, "ignored", "ignored")));
        assertFalse(PortalProductionRequest.isValid(new PortalProductionRequest("", null, "", "ignored", "ignored")));
        assertFalse(PortalProductionRequest.isValid(new PortalProductionRequest("", null, null, "ignored", "ignored")));
        assertTrue(PortalProductionRequest.isValid(new PortalProductionRequest("a", "b", "c", "ignored", "ignored")));
    }

    /**
     * This test will send a request to the server using the greetServer method in
     * BackendService and verify the response.
     */
    public void testProcessingService() {
        // Create the service that we will test.
        BackendServiceAsync backendService = GWT.create(BackendService.class);
        ServiceDefTarget target = (ServiceDefTarget) backendService;
        target.setServiceEntryPoint(GWT.getModuleBaseURL() + "calvalus/backend");

        // Since RPC calls are asynchronous, we will need to wait for a response
        // after this test method returns. This line tells the test runner to wait
        // up to 10 seconds before timing out.
        delayTestFinish(10000);

        // Send a request to the server.
        backendService.orderProduction(new PortalProductionRequest("a", "b", "u", "1.0", "c=d\ne=f"),
                                       new AsyncCallback<PortalProductionResponse>() {
                                           public void onSuccess(PortalProductionResponse response) {
                                               // Verify that the response is correct.
                                               assertTrue(response.getMessage().startsWith("About to process"));

                                               // Now that we have received a response, we need to tell the test runner
                                               // that the test is complete. You must call finishTest() after an
                                               // asynchronous test finishes successfully, or the test will time out.
                                               finishTest();
                                           }

                                           public void onFailure(Throwable caught) {
                                               // The request resulted in an unexpected error.
                                               fail("Request failure: " + caught.getMessage());
                                           }

                                       });
    }


}
