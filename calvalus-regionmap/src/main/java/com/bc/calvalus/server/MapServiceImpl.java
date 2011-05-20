package com.bc.calvalus.server;

import com.bc.calvalus.client.MapService;
import com.bc.calvalus.shared.EncodedRegion;
import com.google.gwt.user.server.rpc.RemoteServiceServlet;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

/**
 * The server side implementation of the RPC service.
 */
@SuppressWarnings("serial")
public class MapServiceImpl extends RemoteServiceServlet implements MapService {

    public EncodedRegion[] getRegions() throws IOException {
        Properties properties = loadRegions();
        ArrayList<EncodedRegion> regions = new ArrayList<EncodedRegion>();
        Set<String> regionNames = properties.stringPropertyNames();
        for (String regionName : regionNames) {
            String regionWKT = properties.getProperty(regionName);
            EncodedRegion region = new EncodedRegion(regionName, regionWKT);
            regions.add(region);
        }
        return regions.toArray(new EncodedRegion[regions.size()]);
    }

    private Properties loadRegions() throws IOException {
        Properties properties = new Properties();
        InputStream stream = MapServiceImpl.class.getResourceAsStream("regions.properties");
        try {
            properties.load(stream);
        } finally {
            stream.close();
        }
        return properties;
    }

    /**
     * Escape an html string. Escaping data received from the client helps to
     * prevent cross-site script vulnerabilities.
     *
     * @param html the html string to escape
     * @return the escaped string
     */
    private String escapeHtml(String html) {
        if (html == null) {
            return null;
        }
        return html.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(
                ">", "&gt;");
    }
}
