package com.bc.calvalus.production;


import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.xml.XmlConvertible;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.binding.ConversionException;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A production request. Production requests are submitted to the backend service.
 *
 * @author Norman
 */
public class ProductionRequest implements XmlConvertible {

    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateFormat DATE_FORMAT = DateUtils.createDateFormat(DATE_PATTERN);

    @Parameter
    private String productionType;
    @Parameter
    private String userName;
    @Parameter(domConverter = HashMapDomConverter.class)
    private Map<String, String> productionParameters;


    // for ProductionRequest.fromXml(String)
    private ProductionRequest() {
    }

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
        this.productionParameters = new HashMap<>(productionParameters);
        this.productionParameters.getClass();
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

    public String getParameter(String name, boolean mandatory) throws ProductionException {
        String value = productionParameters.get(name);
        if (value == null && mandatory) {
            throw new ProductionException("Production parameter '" + name + "' not set.");
        }
        return value;
    }

    public void setParameter(String key, String value) {
        productionParameters.put(key, value);
    }

    public void ensureParameterSet(String name) throws ProductionException {
        getString(name);
    }

    public static DateFormat getDateFormat() {
        return DATE_FORMAT;
    }

    /////////////////////////////////////////////////////////////////////////
    // Support for parameters of different types

    /**
     * Gets a mandatory String parameter value.
     *
     * @param name The parameter name.
     *
     * @return The parameter value.
     *
     * @throws ProductionException If the parameter does not exists or cannot be converted to the requested type.
     */
    public String getString(String name) throws ProductionException {
        return getParameter(name, true);
    }

    /**
     * Gets an optional String parameter value.
     *
     * @param name The parameter name.
     * @param def  The parameter default value.
     */
    public String getString(String name, String def) {
        String value = productionParameters.get(name);
        return value != null ? value : def;
    }

    /**
     * Gets a mandatory Boolean parameter value.
     *
     * @param name The parameter name.
     *
     * @return The parameter value.
     *
     * @throws ProductionException If the parameter does not exists or cannot be converted to the requested type.
     */
    public boolean getBoolean(String name) throws ProductionException {
        return parseBoolean(name, getParameter(name, true));
    }

    /**
     * Gets an optional Boolean parameter value.
     *
     * @param name The parameter name.
     * @param def  The parameter default value.
     *
     * @throws ProductionException If the parameter cannot be converted to the requested type.
     */
    public Boolean getBoolean(String name, Boolean def) throws ProductionException {
        String text = getParameter(name, false);
        if (text != null) {
            return parseBoolean(name, text);
        } else {
            return def;
        }
    }

    /**
     * Gets a mandatory integer parameter value.
     *
     * @param name The parameter name.
     *
     * @return The parameter value.
     *
     * @throws ProductionException If the parameter does not exists or cannot be converted to the requested type.
     */
    public int getInteger(String name) throws ProductionException {
        return parseInteger(name, getParameter(name, true));
    }

    /**
     * Gets an optional integer parameter value.
     *
     * @param name The parameter name.
     * @param def  The parameter default value.
     *
     * @throws ProductionException If the parameter cannot be converted to the requested type.
     */
    public Integer getInteger(String name, Integer def) throws ProductionException {
        String text = getParameter(name, false);
        return text != null ? parseInteger(name, text) : def;
    }

    /**
     * Gets a mandatory single precision parameter value.
     *
     * @param name The parameter name.
     *
     * @return The parameter value.
     *
     * @throws ProductionException If the parameter does not exists or cannot be converted to the requested type.
     */
    public float getFloat(String name) throws ProductionException {
        return parseFloat(name, getParameter(name, true));
    }

    /**
     * Gets an optional single precision parameter value.
     *
     * @param name The parameter name.
     * @param def  The parameter default value.
     *
     * @throws ProductionException If the parameter cannot be converted to the requested type.
     */
    public Float getFloat(String name, Float def) throws ProductionException {
        String text = getParameter(name, false);
        if (text != null) {
            return parseFloat(name, text);
        } else {
            return def;
        }
    }


    /**
     * Gets a mandatory double precision parameter value.
     *
     * @param name The parameter name.
     *
     * @return The parameter value.
     *
     * @throws ProductionException If the parameter does not exists or cannot be converted to the requested type.
     */
    public double getDouble(String name) throws ProductionException {
        return parseDouble(name, getParameter(name, true));
    }

    /**
     * Gets an optional double precision parameter value.
     *
     * @param name The parameter name.
     * @param def  The parameter default value.
     *
     * @throws ProductionException If the parameter cannot be converted to the requested type.
     */
    public Double getDouble(String name, Double def) throws ProductionException {
        String text = getParameter(name, false);
        if (text != null) {
            return parseDouble(name, text);
        } else {
            return def;
        }
    }

    /**
     * Gets a mandatory date parameter value.
     *
     * @param name The parameter name.
     *
     * @return The parameter value.
     *
     * @throws ProductionException If the parameter does not exists or cannot be converted to the requested type.
     */
    public Date getDate(String name) throws ProductionException {
        return parseDate(name, getParameter(name, true));
    }

    /**
     * Gets an optional date parameter value.
     *
     * @param name The parameter name.
     * @param def  The parameter default value.
     *
     * @throws ProductionException If the parameter cannot be converted to the requested type.
     */
    public Date getDate(String name, Date def) throws ProductionException {
        String text = getParameter(name, false);
        return text != null ? parseDate(name, text) : def;
    }

    public Date[] getDates(String name, Date[] def) throws ProductionException {
        String text = getParameter(name, false);
        if (text == null) {
            return def;
        }

        String[] splits = text.trim().split("\\s");
        Set<String> dateSet = new TreeSet<>(Arrays.asList(splits));
        dateSet.remove("");
        splits = dateSet.toArray(new String[dateSet.size()]);
        Arrays.sort(splits);

        Date[] dates = new Date[splits.length];
        for (int i = 0; i < splits.length; i++) {
            dates[i] = parseDate(name, splits[i]);
        }

        return dates;
    }


    /**
     * Gets a mandatory geometry parameter value.
     *
     * @param name The parameter name.
     *
     * @return The parameter value.
     *
     * @throws ProductionException If the parameter does not exists or cannot be converted to the requested type.
     */
    public Geometry getGeometry(String name) throws ProductionException {
        return parseGeometry(name, getParameter(name, true));
    }

    /**
     * Gets an optional geometry parameter value.
     *
     * @param name The parameter name.
     * @param def  The parameter default value.
     *
     * @throws ProductionException If the parameter cannot be converted to the requested type.
     */
    public Geometry getGeometry(String name, Geometry def) throws ProductionException {
        String text = getParameter(name, false);
        return text != null ? parseGeometry(name, text) : def;
    }

    /////////////////////////////////////////////////////////////////////////
    // Support for specific parameters

    public String getProductionName(String defaultProductionName) {
        return getString("productionName", defaultProductionName);
    }

    public boolean isAutoStaging() throws ProductionException {
        return getBoolean("autoStaging", false);
    }

    public String getStagingDirectory(String productionId) {
        String remoteUser;
        try {
            remoteUser = getParameter("calvalus.wps.remote.user", true);
        } catch (ProductionException exception) {
            return userName + "/" + productionId;
        }
        if (remoteUser.equalsIgnoreCase(userName)) {
            return userName + "/" + productionId;
        } else {
            return userName + "/" + remoteUser + "/" + productionId;
        }
    }

    public String getRegionName() {
        return getString("regionName", "");
    }

    public Geometry getRegionGeometry() throws ProductionException {
        Geometry regionGeometry = getRegionGeometry(null);
        if (regionGeometry == null) {
            throw new ProductionException(
                        "Missing region geometry, either parameter 'regionWKT' or 'minLon', 'minLat','maxLon','maxLat' must be provided");
        }
        return regionGeometry;
    }

    public Geometry getRegionGeometry(Geometry defaultGeometry) throws ProductionException {
        Geometry geometry = getGeometry("regionWKT", null);
        if (geometry != null) {
            return geometry;
        }
        Double x1 = getDouble("minLon", null);
        Double y1 = getDouble("minLat", null);
        Double x2 = getDouble("maxLon", null);
        Double y2 = getDouble("maxLat", null);
        if (x1 != null && y1 != null && x2 != null && y2 != null) {
            GeometryFactory factory = new GeometryFactory();
            return factory.createPolygon(factory.createLinearRing(new Coordinate[]{
                        new Coordinate(x1, y1),
                        new Coordinate(x2, y1),
                        new Coordinate(x2, y2),
                        new Coordinate(x1, y2),
                        new Coordinate(x1, y1),
                        }), null);
        } else if (x1 == null && y1 == null && x2 == null && y2 == null) {
            return defaultGeometry;
        } else {
            throw new ProductionException("Parameters 'minLon', 'minLat','maxLon','maxLat' must all be given");
        }
    }

    public List<DateRange> getDateRanges() throws ProductionException {
        List<DateRange> dateRangeList = new ArrayList<>();
        Date[] dateList = getDates("dateList", null);
        if (dateList != null) {
            Arrays.sort(dateList);
            for (Date date : dateList) {
                dateRangeList.add(new DateRange(date, date));
            }
        } else {
            Date minDate = getDate("minDate", null);
            Date maxDate = getDate("maxDate", null);
            dateRangeList.add(new DateRange(minDate, maxDate));
        }
        return dateRangeList;
    }

    public DateRange createFromMinMax() throws ProductionException {
        Date minDate = getDate("minDate");
        Date maxDate = getDate("maxDate");
        return new DateRange(minDate, maxDate);
    }
    
    /**
     * Gets an optional String parameter value.
     * If the string parameters exist the value is XML decoded.
     * This is the reverse to @see com.bc.calvalus.portal.client.ParametersEditorGenerator#encodeXML(java.lang.String)
     *
     * @param name The parameter name.
     * @param def  The parameter default value.
     */
    public String getXmlDecodedString(String name, String def) {
        String s = getString(name, def);
        return s != null ? decodeXML(s) : def;
    }

    private static String decodeXML(String s) {
        return s.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&");
    }

    /////////////////////////////////////////////////////////////////////////
    // XmlConvertible

    @Override
    public String toXml() {
        try {
            return new ParameterBlockConverter().convertObjectToXml(this);
        } catch (ConversionException e) {
            throw new RuntimeException(e);
        }
    }

    public static ProductionRequest fromXml(String xml) throws BindingException {
        if (xml != null) {
            return new ParameterBlockConverter().convertXmlToObject(xml, new ProductionRequest());
        }
        return null;
    }

    /////////////////////////////////////////////////////////////////////////
    // Implementation helpers

    private static Map<String, String> mapify(String[] parametersKeyValuePairs) {
        Map<String, String> productionParameters = new HashMap<>();
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

    private static boolean parseBoolean(String name, String text) throws ProductionException {
        if (text.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        } else if (text.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        } else {
            throw new ProductionException("Production parameter '" + name + "' must be a Boolean ('true' or 'false').");
        }
    }

    private static Integer parseInteger(String name, String text) throws ProductionException {
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException e) {
            throw new ProductionException("Production parameter '" + name + "' must be an integer number.");
        }
    }

    private static float parseFloat(String name, String text) throws ProductionException {
        try {
            return Float.parseFloat(text);
        } catch (NumberFormatException e) {
            throw new ProductionException("Production parameter '" + name + "' must be a number.");
        }
    }

    private static double parseDouble(String name, String text) throws ProductionException {
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException e) {
            throw new ProductionException("Production parameter '" + name + "' must be a number.");
        }
    }

    private static Date parseDate(String name, String text) throws ProductionException {
        try {
            return DATE_FORMAT.parse(text);
        } catch (ParseException e) {
            throw new ProductionException("Parameter '" + name + "' must be date of format '" + DATE_PATTERN + "'.");
        }
    }

    private static Geometry parseGeometry(String name, String text) throws ProductionException {
        final WKTReader wktReader = new WKTReader();
        try {
            return wktReader.read(text);
        } catch (com.vividsolutions.jts.io.ParseException e) {
            throw new ProductionException(
                        "Production parameter '" + name + "' must be a geometry (ISO 19107 WKT format).");
        }
    }
}
