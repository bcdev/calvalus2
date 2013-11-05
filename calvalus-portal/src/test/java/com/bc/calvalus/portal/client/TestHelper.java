package com.bc.calvalus.portal.client;

import com.google.gwt.maps.client.LoadApi;

/**
 * @author Marco Peters
 */
public class TestHelper {

    public static void assertMapsApiLoaded() {
        final long t0 = System.currentTimeMillis();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                final long t1 = System.currentTimeMillis();
                long l = t1 - t0;
                System.out.println("MapsApi loaded after " + l + " ms");
            }
        };
        LoadApi.go(runnable, false);
    }
}
