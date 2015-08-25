package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusConfig;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProductionService;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Created by hans on 24/08/2015.
 */
@Path("/Test")
public class GetTest {

    @Context ServletContext context;

    public GetTest(@Context ServletContext context) {
        this.context = context;
    }

    @GET
    @Path("{productionId}")
    @Produces(MediaType.TEXT_PLAIN)
    public String execute(@PathParam("productionId") String productionid) {
//        if(context != null){
//            return (String) context.getAttribute("var1");
//        }
        StaticTest staticTest = new StaticTest();
        return StaticTest.key;
    }

}
