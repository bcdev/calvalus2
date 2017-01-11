package com.bc.calvalus.generator.extractor;

import com.bc.calvalus.commons.CalvalusLogger;
import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Level;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;


/**
 * @author muhammad.bc.
 */

public abstract class ReaderHistorySource implements Closeable {

    private static final int MSG_CODE_SUCCESSFUL_START = 200;
    private static final int MSG_CODE_SUCCESSFUL_END = 300;
    private final Client clientRequest;
    private boolean isConnected;
    private Invocation.Builder builder;

    ReaderHistorySource(String sourceUrl) {
        clientRequest = ClientBuilder.newClient();
        try {
            builder = sendRequestBuilder(sourceUrl);
            Response response = builder.get();
            if (MSG_CODE_SUCCESSFUL_START <= response.getStatus() || response.getStatus() < MSG_CODE_SUCCESSFUL_END) {
                isConnected = true;
            } else {
                throw new InternalServerErrorException();
            }
        } catch (IllegalArgumentException | InternalServerErrorException | ProcessingException exception) {
            CalvalusLogger.getLogger().log(Level.SEVERE, "Url is not correct or their is no connection");
        }
    }

    public final boolean isConnect() {
        return isConnected;
    }

    public void close() throws IOException {
        clientRequest.close();
    }

    private Invocation.Builder sendRequestBuilder(String sourceUrl) {
        return clientRequest.target(sourceUrl).request();
    }

    String readSource(FormatType formatType) {
        Invocation.Builder accept = builder.accept(formatType.getFormat());
        return accept.get(String.class);
    }
}
