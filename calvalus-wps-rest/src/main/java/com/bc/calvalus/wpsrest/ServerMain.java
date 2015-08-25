package com.bc.calvalus.wpsrest;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.grizzly.http.server.accesslog.AccessLogBuilder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Main class.
 */
public class ServerMain {

    // Base URI the Grizzly HTTP server will listen on
    public static final String BASE_URI = "http://localhost:8089/calvalus-wps/";

    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     *
     * @param serverUri
     *
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer(String serverUri) throws IOException {
        // create a resource config that scans for JAX-RS resources and services
        // in com.bc.calvalus.wpsrest.services package
        final ResourceConfig rc = new ResourceConfig().packages("com.bc.calvalus.wpsrest.services");
        Map<String, Object> properties = new HashMap<>();
        properties.put("jersey.config.server.tracing.type ", "ALL");
        rc.addProperties(properties);

        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(URI.create(serverUri), rc, false);

        ServerConfiguration serverConfiguration = httpServer.getServerConfiguration();

        final AccessLogBuilder builder = new AccessLogBuilder("access.log");
        builder.instrument(serverConfiguration);

        httpServer.start();
        return httpServer;
    }

    /**
     * Main method.
     *
     * @param args Command line argument
     *
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String serverUri = args.length == 2 ? args[0] : BASE_URI;

        final HttpServer server = startServer(serverUri);
        System.out.println("Jersey app started");
    }
}

