package com.bc.calvalus.ui.client;

import com.bc.calvalus.ui.shared.ProcessingRequest;
import com.google.gwt.core.client.GWT;
import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.ServiceDefTarget;

public class CalvalusAppTest extends GWTTestCase {

    /**
     * Must refer to a valid module that sources this class.
     */
    public String getModuleName() {
        return "com.bc.calvalus.ui.CalvalusAppJUnit";
    }

    /**
     * Tests the ProcessingRequestVerifier.
     */
    public void testFieldVerifier() {
        assertFalse(ProcessingRequest.isValid(new ProcessingRequest(null, null, null, "ignored")));
        assertFalse(ProcessingRequest.isValid(new ProcessingRequest("", null, "", "ignored")));
        assertFalse(ProcessingRequest.isValid(new ProcessingRequest("", null, null, "ignored")));
        assertTrue(ProcessingRequest.isValid(new ProcessingRequest("a", "b", "c", "ignored")));
    }

    /**
     * This test will send a request to the server using the greetServer method in
     * ProcessingService and verify the response.
     */
    public void testProcessingService() {
        // Create the service that we will test.
        ProcessingServiceAsync processingService = GWT.create(ProcessingService.class);
        ServiceDefTarget target = (ServiceDefTarget) processingService;
        target.setServiceEntryPoint(GWT.getModuleBaseURL() + "calvalusapp/process");

        // Since RPC calls are asynchronous, we will need to wait for a response
        // after this test method returns. This line tells the test runner to wait
        // up to 10 seconds before timing out.
        delayTestFinish(10000);

        // Send a request to the server.
        processingService.process(new ProcessingRequest("a", "b", "u", "c=d\ne=f"), new AsyncCallback<String>() {
            public void onFailure(Throwable caught) {
                // The request resulted in an unexpected error.
                fail("Request failure: " + caught.getMessage());
            }

            public void onSuccess(String result) {
                // Verify that the response is correct.
                assertTrue(result.startsWith("About to process"));

                // Now that we have received a response, we need to tell the test runner
                // that the test is complete. You must call finishTest() after an
                // asynchronous test finishes successfully, or the test will time out.
                finishTest();
            }
        });
    }


}
