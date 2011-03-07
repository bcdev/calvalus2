package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

/**
 * Created by IntelliJ IDEA.
 * User: Norman
 * Date: 07.03.11
 * Time: 11:00
 * To change this template use File | Settings | File Templates.
 */
public interface ProductionType {
    String getName();
    HadoopProduction createProduction(ProductionRequest pdr) throws ProductionException ;
    void stageProduction(HadoopProduction  p) throws ProductionException;
}
