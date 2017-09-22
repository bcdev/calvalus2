package com.bc.calvalus.rest;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.rest.exceptions.CalvalusProcessorNotFoundException;
import com.bc.calvalus.rest.exceptions.InvalidProcessorIdException;
import com.bc.wps.utilities.PropertiesWrapper;
import com.google.gson.Gson;
import org.apache.commons.lang.ArrayUtils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
@Path("/")
public class CalvalusService {

    private final CalvalusFacade calvalusFacade;
    private final Gson gson;

    public CalvalusService() throws IOException {
        PropertiesWrapper.loadConfigFile("calvalus-rest.properties");
        calvalusFacade = new CalvalusFacade();
        gson = new Gson();
    }

    @GET
    @Path("test")
    @Produces(MediaType.APPLICATION_JSON)
    public String getTestEndpoint() throws IOException, ProductionException {
        return "Hello world!";
    }

    @GET
    @Path("input-dataset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getInputDataset() throws IOException, ProductionException, InvalidProcessorIdException, CalvalusProcessorNotFoundException {
        ProcessorNameConverter converter = new ProcessorNameConverter("urbantep-subsetting~1.0~Subset");
        CalvalusProcessor processor = calvalusFacade.getProcessor(converter, "hans");
        ProductSet[] allProductSets = calvalusFacade.getProductSets("hans");
        List<ProductSet> productSets = new ArrayList<>();
        for (ProductSet productSet : allProductSets) {
            if (ArrayUtils.contains(processor.getInputProductTypes(), productSet.getProductType())) {
                productSets.add(productSet);
            }
        }
        return Response.ok().entity(gson.toJson(productSets)).build();
    }
}
