package com.bc.calvalus.wps2;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusConfig;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusProcessorExtractor;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusProductionService;
import org.junit.*;

import java.util.List;

/**
 * Created by hans on 13/08/2015.
 */
public class CalvalusProcessorExtractorTest {

    @Ignore
    @Test
    public void testGetProcessor() throws Exception {
        CalvalusProductionService calvalusProductionService = new CalvalusProductionService();
        CalvalusConfig calvalusConfig = new CalvalusConfig();
        ProductionService productionService = calvalusProductionService.createProductionService(calvalusConfig);
        CalvalusProcessorExtractor extractor = new CalvalusProcessorExtractor(productionService);
        List<Processor> processors = extractor.getProcessors();
        System.out.println("processorDescriptors.size() = " + processors.size());
        for (Processor processor : processors) {
            System.out.println(processor.getIdentifier());
//            System.out.println("processor.getTitle() = " + processor.getTitle());
//            System.out.println();
        }
    }

    @Ignore
    @Test
    public void testGetProductSet() throws Exception {
        CalvalusProductionService calvalusProductionService = new CalvalusProductionService();
        CalvalusConfig calvalusConfig = new CalvalusConfig();
        ProductionService productionService = calvalusProductionService.createProductionService(calvalusConfig);
        CalvalusProcessorExtractor extractor = new CalvalusProcessorExtractor(productionService);

        ProductSet[] productSets = extractor.getProductSets();
        System.out.println("productSets.length = " + productSets.length);
        for (ProductSet productSet : productSets) {
            System.out.println("productSet.getName() = " + productSet.getName());
            System.out.println("productSet.getPath() = " + productSet.getPath());
            System.out.println("productSet.getProductType() = " + productSet.getProductType());
            System.out.println("productSet.getRegionName() = " + productSet.getRegionName());
            System.out.println("productSet.getRegionWKT() = " + productSet.getRegionWKT());
            System.out.println("productSet.getMaxDate() = " + productSet.getMaxDate());
            System.out.println("productSet.getMinDate() = " + productSet.getMinDate());
            System.out.println();
        }

    }
}