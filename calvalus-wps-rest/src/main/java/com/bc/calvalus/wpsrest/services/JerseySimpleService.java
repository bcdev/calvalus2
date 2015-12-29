package com.bc.calvalus.wpsrest.services;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * @author hans
 */
@Path("/test")
public class JerseySimpleService {

    @GET
    @Path("{id}")
    public String getResponse(@PathParam("id") String id) {
        System.out.println(id);
        return id;
    }

    @GET
    public String getResponseWithParam(@QueryParam("Service") String service) {
        return "service : " + service;
    }

    @POST
    public String postResponse() {
        return "post successful";
    }

}
