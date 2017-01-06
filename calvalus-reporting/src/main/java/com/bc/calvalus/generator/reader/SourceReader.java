package com.bc.calvalus.generator.reader;

import java.io.Closeable;
import java.io.IOException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;


/**
 * @author muhammad.bc.
 */

public abstract class SourceReader implements Closeable {

    public static final int MSG_CODE_SUCCESSFUL_START = 200;
    public static final int MSG_CODE_SUCCESSFUL_END = 300;
    private final Client clientRequest;
    private boolean isConnected;
    private Invocation.Builder builder;

    public SourceReader(String sourceUrl) {
        clientRequest = ClientBuilder.newClient();
        try {
            builder = connectToSource(sourceUrl);
            Response response = builder.get();
            if (MSG_CODE_SUCCESSFUL_START <= response.getStatus() || response.getStatus() < MSG_CODE_SUCCESSFUL_END) {
                isConnected = true;
            } else {
                throw new InternalServerErrorException();
            }
        } catch (IllegalArgumentException | InternalServerErrorException | ProcessingException exception) {
            throw new IllegalArgumentException("Check the url path for extracting the log file.");
        }
    }

    private final Invocation.Builder connectToSource(String sourceUrl) {
        return clientRequest.target(sourceUrl).request();
    }

    protected final String readSource(FormatType formatType) {
        Invocation.Builder accept = builder.accept(formatType.getFormat());
        String result = accept.get(String.class);
        return result;
    }

    public final boolean isConnect() {
        return isConnected;
    }

    public void close() throws IOException {
        clientRequest.close();
    }
}
