package com.bc.calvalus.production;


import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.beam.framework.datamodel.ProductData;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * A production request. Production requests are submitted to the backend service.
 *
 * @author Norman
 */
public class ProductionRequest {
    public final static DateFormat dateFormat = ProductData.UTC.createDateFormat("yyyy-MM-dd");
    private final String productionType;
    private final String userName;
    private final Map<String, String> productionParameters;

    public ProductionRequest(String productionType,
                             String userName,
                             String... productionParametersKeyValuePairs) {
        this(productionType, userName, mapify(productionParametersKeyValuePairs));
    }

    public ProductionRequest(String productionType,
                             String userName,
                             Map<String, String> productionParameters) {
        if (productionType == null) {
            throw new NullPointerException("productionType");
        }
        if (productionType.isEmpty()) {
            throw new IllegalArgumentException("productionType.isEmpty()");
        }
        if (userName == null) {
            throw new NullPointerException("userName");
        }
        if (userName.isEmpty()) {
            throw new IllegalArgumentException("userName.isEmpty()");
        }
        if (productionParameters == null) {
            throw new NullPointerException("productionParameters");
        }
        this.productionType = productionType;
        this.userName = userName;
        this.productionParameters = new HashMap<String, String>(productionParameters);
    }

    public String getProductionType() {
        return productionType;
    }

    public String getUserName() {
        return userName;
    }

    public Map<String, String> getParameters() {
        return Collections.unmodifiableMap(productionParameters);
    }

    public String getParameter(String name) {
        return productionParameters.get(name);
    }

    public String getParameterSafe(String name) throws ProductionException {
        String value = getParameter(name);
        if (value == null) {
            throw new ProductionException("Missing production parameter '" + name + "'");
        }
        return value;
    }

    public void ensureParameterSet(String name) throws ProductionException {
        getParameterSafe(name);
    }

    public static DateFormat getDateFormat() {
        return dateFormat;
    }

    /////////////////////////////////////////////////////////////////////////
    // Support for parameters of different types

    public boolean getBoolean(String name, Boolean def) {
        String text = getParameter(name);
        if (text != null) {
            return Boolean.parseBoolean(text);
        } else {
            return def;
        }
    }

    public Integer getInteger(String name, Integer def) {
        String text = getParameter(name);
        if (text != null) {
            return Integer.parseInt(text);
        } else {
            return def;
        }
    }

    public Double getDouble(String name, Double def) {
        String text = getParameter(name);
        if (text != null) {
            return Double.parseDouble(text);
        } else {
            return def;
        }
    }

    public Date getDate(String name) throws ProductionException {
        try {
            return dateFormat.parse(getParameterSafe(name));
        } catch (ParseException e) {
            throw new ProductionException("Illegal date format for production parameter '" + name + "'");
        }
    }


    /////////////////////////////////////////////////////////////////////////
    // Support for specific parameters

    public Geometry getRegionGeometry() throws ProductionException {
        String regionWKT = getParameter("regionWKT");
        if (regionWKT == null) {
            String x1 = getParameterSafe("lonMin");
            String y1 = getParameterSafe("latMin");
            String x2 = getParameterSafe("lonMax");
            String y2 = getParameterSafe("latMax");
            regionWKT = String.format("POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))",
                                      x1, y1,
                                      x2, y1,
                                      x2, y2,
                                      x1, y2,
                                      x1, y1);
        }
        final WKTReader wktReader = new WKTReader();
        try {
            return wktReader.read(regionWKT);
        } catch (com.vividsolutions.jts.io.ParseException e) {
            throw new IllegalArgumentException("Illegal region geometry: " + regionWKT, e);
        }
    }

    /////////////////////////////////////////////////////////////////////////
    // Implementation helpers

    private static Map<String, String> mapify(String[] parametersKeyValuePairs) {
        Map<String, String> productionParameters = new HashMap<String, String>();
        for (int i = 0; i < parametersKeyValuePairs.length; i += 2) {
            String name = parametersKeyValuePairs[i];
            if (name == null) {
                throw new NullPointerException("name");
            }
            String value = parametersKeyValuePairs[i + 1];
            if (value == null) {
                throw new NullPointerException("value");
            }
            productionParameters.put(name, value);
        }
        return productionParameters;
    }
}
