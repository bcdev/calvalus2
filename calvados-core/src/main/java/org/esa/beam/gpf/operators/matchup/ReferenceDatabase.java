package org.esa.beam.gpf.operators.matchup;

import org.esa.beam.framework.datamodel.Product;

import java.util.List;


public interface ReferenceDatabase {

    List<ReferenceMeasurement> findReferenceMeasurement(String site, Product product);
}
