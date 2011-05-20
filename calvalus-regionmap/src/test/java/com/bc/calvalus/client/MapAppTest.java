package com.bc.calvalus.client;

import com.bc.calvalus.shared.EncodedRegion;
import com.google.gwt.core.client.GWT;
import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.user.client.rpc.AsyncCallback;
import com.google.gwt.user.client.rpc.ServiceDefTarget;

/**
 * GWT JUnit tests must extend GWTTestCase.
 */
public class MapAppTest extends GWTTestCase {

    /**
     * Must refer to a valid module that sources this class.
     */
    public String getModuleName() {
        return "com.bc.calvalus.MapAppJUnit";
    }


    /**
     * This test will send a request to the server using the greetServer method in
     * GreetingService and verify the response.
     */
    public void testGreetingService() {
        // Create the service that we will test.
        MapServiceAsync greetingService = GWT.create(MapService.class);
        ServiceDefTarget target = (ServiceDefTarget) greetingService;
        target.setServiceEntryPoint(GWT.getModuleBaseURL() + "mapapp/greet");

        // Since RPC calls are asynchronous, we will need to wait for a response
        // after this test method returns. This line tells the test runner to wait
        // up to 10 seconds before timing out.
        delayTestFinish(10000);

        // Send a request to the server.
        greetingService.getRegions(new AsyncCallback<EncodedRegion[]>() {
            public void onFailure(Throwable caught) {
                fail("Request failure: " + caught.getMessage());
            }

            public void onSuccess(EncodedRegion[] result) {
                assertNotNull(result);
                assertEquals(29, result.length);
                finishTest();
            }
        });
    }


}
