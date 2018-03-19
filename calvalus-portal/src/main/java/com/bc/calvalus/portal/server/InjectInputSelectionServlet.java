package com.bc.calvalus.portal.server;

import static com.bc.calvalus.portal.server.BCAuthenticationFilter.PAYLOAD_PREFIX;

import com.bc.calvalus.portal.shared.DtoDateRange;
import com.bc.calvalus.portal.shared.DtoInputSelection;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import org.geojson.Feature;
import org.geojson.GeoJsonObject;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.geojson.Polygon;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class InjectInputSelectionServlet extends HttpServlet {

    private static final String FORWARD_URL = "/close-window.jsp";
    private static final String CATALOGUE_SEARCH_PREFIX = "catalogueSearch_";

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
    protected void doPost(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
        String requestPayload = servletRequest.getParameter("request");
        System.out.println("Request payload: " + requestPayload);
        DtoInputSelection dtoInputSelection = getDtoInputSelectionFromJson(requestPayload);
        Principal userPrincipal = servletRequest.getUserPrincipal();
        if (userPrincipal != null) {
            getServletContext().setAttribute(CATALOGUE_SEARCH_PREFIX + userPrincipal.getName(), dtoInputSelection);
        }
        closeBrowserWindow(servletRequest, servletResponse);
    }

    @Override
    protected void doGet(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
        HttpSession session = servletRequest.getSession();
        String payload = "";
        if (session != null) {
            String sessionId = session.getId();
            payload = (String) session.getAttribute(PAYLOAD_PREFIX + sessionId);
            System.out.println("Request payload: " + payload);
        }
        DtoInputSelection dtoInputSelection = getDtoInputSelectionFromJson(payload);
        Principal userPrincipal = servletRequest.getUserPrincipal();
        if (userPrincipal != null) {
            getServletContext().setAttribute(CATALOGUE_SEARCH_PREFIX + userPrincipal.getName(), dtoInputSelection);
        }
        closeBrowserWindow(servletRequest, servletResponse);
    }

    private void closeBrowserWindow(HttpServletRequest servletRequest, HttpServletResponse servletResponse) {
        RequestDispatcher rd = getServletContext().getRequestDispatcher(FORWARD_URL);
        try {
            rd.forward(servletRequest, servletResponse);
        } catch (ServletException | IOException exception) {
            System.err.println("Unable to forward to " + FORWARD_URL);
            exception.printStackTrace();
        }
    }

    private DtoInputSelection getDtoInputSelectionFromJson(String payload) {
        JsonDeserializer<DtoInputSelection> deserializer = (jsonElement, type, jsonDeserializationContext) -> {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            JsonElement featureObject = jsonObject.getAsJsonObject("regionGeometry");
            String featureString = featureObject.toString();
            String regionWkt;
            try {
                GeoJsonObject geoJsonObject = new ObjectMapper().readValue(featureString, GeoJsonObject.class);
                regionWkt = extractRegionWkt(geoJsonObject);
            } catch (IOException exception) {
                throw new JsonParseException(exception);
            }

            Type listType = new TypeToken<ArrayList<String>>() {
            }.getType();
            List<String> productIdentifierList = new Gson().fromJson(jsonObject.get("productIdentifiers"), listType);

            DtoDateRange dateRange = new Gson().fromJson(jsonObject.get("dateRange"), DtoDateRange.class);

            return new DtoInputSelection(jsonObject.get("collectionName").getAsString(),
                                         productIdentifierList,
                                         dateRange,
                                         regionWkt
            );
        };
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(DtoInputSelection.class, deserializer);
        Gson gson = gsonBuilder.create();

        return gson.fromJson(payload, DtoInputSelection.class);
    }

    private String extractRegionWkt(GeoJsonObject geoJsonObject) {
        if (geoJsonObject instanceof Feature) {
            Feature feature = (Feature) geoJsonObject;
            geoJsonObject = feature.getGeometry();
        }

        String regionWkt = "";

        if (geoJsonObject instanceof Point) {
            Point point = (Point) geoJsonObject;
            regionWkt = convertPointToWkt(point);
        } else if (geoJsonObject instanceof Polygon) {
            Polygon polygon = (Polygon) geoJsonObject;
            regionWkt = convertPolygonToWkt(polygon);
        }
        return regionWkt;
    }

    private String convertPolygonToWkt(Polygon polygon) {
        StringWriter writer = new StringWriter();
        writer.append("POLYGON((");
        List<List<LngLatAlt>> coordinates = polygon.getCoordinates();
        for (LngLatAlt coordinate : coordinates.get(0)) {
            writer.append(String.valueOf(coordinate.getLongitude()));
            writer.append(" ");
            writer.append(String.valueOf(coordinate.getLatitude()));
            writer.append(",");
        }
        writer.append("))");
        String regionWkt = writer.toString();
        int lastCommaIndex = regionWkt.lastIndexOf(",");
        regionWkt = regionWkt.substring(0, lastCommaIndex) + regionWkt.substring(lastCommaIndex + 1);
        return regionWkt;
    }

    private String convertPointToWkt(Point point) {
        // TODO: ask MB how to best implement point selection
        System.out.println("=============Point===============");
        LngLatAlt pointCoordinates = point.getCoordinates();
        System.out.println(pointCoordinates);
        System.out.println(pointCoordinates.getLatitude());
        System.out.println(pointCoordinates.getLongitude());
        System.out.println(pointCoordinates.getAltitude());
        System.out.println("=============Point===============");
        return "";
    }
}
