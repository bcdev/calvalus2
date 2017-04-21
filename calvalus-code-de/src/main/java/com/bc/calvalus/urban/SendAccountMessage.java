package com.bc.calvalus.urban;

import com.bc.wps.utilities.PropertiesWrapper;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

/**
 * @author muhammad.bc.
 */

public class SendAccountMessage {
    private static final String url = PropertiesWrapper.get("account.server.url");
    private static final String userName = PropertiesWrapper.get("account.server.username");
    private static final String password = PropertiesWrapper.get("account.server.password");
    private static final String logAccountMessagePath = PropertiesWrapper.get("account.log.send.path");
    private static SendAccountMessage sendAccount;

    private SendAccountMessage() {

        HttpAuthenticationFeature feature = HttpAuthenticationFeature.basicBuilder()
                .nonPreemptive().credentials(userName, password).build();
        Client client = ClientBuilder.newClient();
        client.register(feature);
        Response response = client.target(url).request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(null));


//        builder = ClientBuilder.newClient()
//                .target(url)
//                .property(HTTP_AUTHENTICATION_BASIC_USERNAME, userName)
//                .property(HTTP_AUTHENTICATION_BASIC_PASSWORD, password)
//                .request(MediaType.APPLICATION_JSON_TYPE);

    }

    public static SendAccountMessage getInstance() {
        if (sendAccount == null) {
            sendAccount = new SendAccountMessage();
        }
        return sendAccount;
    }




}
