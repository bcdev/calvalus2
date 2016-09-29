package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.cmd.LdapHelper;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is the facade of any calvalus-related operations.
 *
 * @author hans
 */
public class CalvalusFacade {

    private final String userName;
    private final CalvalusProduction calvalusProduction;
    private final CalvalusStaging calvalusStaging;
    private final CalvalusProcessorExtractor calvalusProcessorExtractor;

    public CalvalusFacade(WpsRequestContext wpsRequestContext) throws IOException {
        this.userName = resolveUserName(wpsRequestContext);
        this.calvalusProduction = new CalvalusProduction();
        this.calvalusStaging = new CalvalusStaging(wpsRequestContext.getServerContext());
        this.calvalusProcessorExtractor = new CalvalusProcessorExtractor();
    }

    public ProductionService getProductionService() throws ProductionException, IOException {
        return CalvalusProductionService.getProductionServiceSingleton();
    }

    public String orderProductionAsynchronous(ProductionRequest request) throws ProductionException, IOException {
        return calvalusProduction.orderProductionAsynchronous(getProductionService(), request, userName);
    }

    public String orderProductionSynchronous(ProductionRequest request) throws ProductionException, InterruptedException, IOException {
        return calvalusProduction.orderProductionSynchronous(getProductionService(), request);
    }

    public List<String> getProductResultUrls(String jobId) throws IOException, ProductionException {
        Production production = getProduction(jobId);
        return calvalusStaging.getProductResultUrls(CalvalusProductionService.getDefaultConfig(), production);
    }

    public void stageProduction(String jobId) throws ProductionException, IOException {
        calvalusStaging.stageProduction(getProductionService(), jobId);
    }

    public void observeStagingStatus(String jobId) throws InterruptedException, IOException, ProductionException {
        Production production = getProduction(jobId);
        calvalusStaging.observeStagingStatus(getProductionService(), production);
    }

    public List<IWpsProcess> getProcessors() throws IOException, ProductionException {
        return calvalusProcessorExtractor.getProcessors(getProductionService(), userName);
    }

    public CalvalusProcessor getProcessor(ProcessorNameConverter parser) throws IOException, ProductionException {
        return calvalusProcessorExtractor.getProcessor(parser, getProductionService(), userName);
    }

    public ProductSet[] getProductSets() throws ProductionException, IOException {
        List<ProductSet> productSets = new ArrayList<>();
        productSets.addAll(Arrays.asList(getProductionService().getProductSets(userName, "")));
        try {
            productSets.addAll(Arrays.asList(getProductionService().getProductSets(userName, "user=" + userName)));
        } catch (ProductionException ignored) {
        }
        return productSets.toArray(new ProductSet[productSets.size()]);
    }

    public Production getProduction(String jobId) throws IOException, ProductionException {
        return getProductionService().getProduction(jobId);
    }

    public String getUserName() {
        return this.userName;
    }

    private String resolveUserName(WpsRequestContext wpsRequestContext) throws IOException {
        String remoteUserName = wpsRequestContext.getHeaderField("remote_user");
        if (remoteUserName != null) {
            remoteUserName = "tep_" + remoteUserName;
            LdapHelper ldap = new LdapHelper();
            if (!ldap.isRegistered(remoteUserName)) {
                ldap.register(remoteUserName);
            }
            return remoteUserName;
        } else {
            return wpsRequestContext.getUserName();
        }
    }
}
