/*
 * Copyright (C) 2019 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.analysis;

import com.bc.calvalus.commons.CalvalusLogger;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.esa.snap.core.datamodel.Product;

import java.io.*;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.logging.Logger;

/**
 * GeoServer REST API handler
 *
 * @author Declan
 */
public class GeoServer {

    private static final Logger LOGGER = CalvalusLogger.getLogger();

    private final TaskAttemptContext context;
    private final Product sourceProduct;
    private final Quicklooks.QLConfig qlConfig;

    public GeoServer(TaskAttemptContext context, Product product, Quicklooks.QLConfig qlConfig) {
        this.context = context;
        this.sourceProduct = product;
        this.qlConfig = qlConfig;
    }

    public void uploadImage(InputStream inputStream, String imageName) throws IOException {

        String msg = String.format("Uploading product image '%s' to GeoServer.", imageName);
        LOGGER.info(msg);

        String geoserverRestURL = this.qlConfig.getGeoServerRestUrl();
        String username = this.qlConfig.getGeoServerUsername();
        String password = this.qlConfig.getGeoServerPassword();
        String workspace = this.qlConfig.getGeoServerWorkspace();
        String store = this.qlConfig.getGeoServerStore();
        String layer = this.qlConfig.getGeoServerLayer();
        String style = this.qlConfig.getGeoServerStyle();

        if (geoserverRestURL == null || geoserverRestURL.isEmpty()) {
            throw new IllegalArgumentException("geoserverRestURL is empty");
        }
        geoserverRestURL = geoserverRestURL.trim();
        while (geoserverRestURL.endsWith("/")) {
            geoserverRestURL = geoserverRestURL.substring(0, geoserverRestURL.length() - 1);
        }

        if (workspace == null || workspace.isEmpty()) {
            throw new IllegalArgumentException("workspace is empty");
        }

        if (store == null || store.isEmpty()) {
            store = imageName;
        }
        else {
            store.trim();
        }

        if (layer == null || layer.isEmpty()) {
            layer = imageName;
        }
        else {
            layer.trim();
        }

        if (username != null && !username.isEmpty() && password != null) {
            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password.toCharArray());
                }
            });
        }

        // other functionality for future (not required at present)
        //getCoverageStore(geoserverURL, workspace, layerID);
        //deleteCoverageStore(geoserverURL, workspace, layerID);

        putCoverageStoreSingleGeoTiff(inputStream, geoserverRestURL, workspace, store, layer);

        if (style != null && !style.isEmpty()) {
            putLayerStyle(geoserverRestURL, workspace, store, style);
        }
    }

    /**
     * Get a coverage store named {storeName} in the {workspace} workspace
     *
     * @param geoserverRestURL  the root URL to GeoServer
     * @param workspace         the name of the workspace containing the coverage stores
     * @param store             the name of the store to be retrieved
     */
    public void getCoverageStore(String geoserverRestURL, String workspace, String store) throws IOException {
        String getCoverageStoreURL = String.format("%s/workspaces/%s/coveragestores/%s", geoserverRestURL, workspace, store);
        LOGGER.info("Getting GeoServer coverage store: " + getCoverageStoreURL);

        URL url = new URL(getCoverageStoreURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(false);
        conn.setUseCaches(false);
        conn.setRequestProperty("Content-Type", "application/xml");
        conn.setRequestProperty("charset", "utf-8");
        conn.setRequestMethod("GET");

        int responseCode = conn.getResponseCode();
        String responseMessage = conn.getResponseMessage();
        LOGGER.info("GeoServer response: " + responseCode + ": " + responseMessage);
        conn.disconnect();
    }

    /**
     * Delete a coverage store named {storeName} in the {workspace} workspace
     *
     * @param geoserverRestURL  the root URL to GeoServer
     * @param workspace         the name of the workspace containing the coverage stores
     * @param store             the name of the store to be deleted
     */
    public void deleteCoverageStore(String geoserverRestURL, String workspace, String store) throws IOException {
        String deleteCoverageStoreURL = String.format("%/workspaces/%s/coveragestores/%s?recurse=true&purge=all", geoserverRestURL, workspace, store);
        LOGGER.info("Deleting GeoServer coverage store: " + deleteCoverageStoreURL);

        URL url = new URL(deleteCoverageStoreURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(false);
        conn.setUseCaches(false);
        //conn.setRequestProperty("Content-Type", "application/xml");
        //conn.setRequestProperty("charset", "utf-8");
        conn.setRequestMethod("DELETE");

        int responseCode = conn.getResponseCode();
        String responseMessage = conn.getResponseMessage();
        LOGGER.info("GeoServer response: " + responseCode + ": " + responseMessage);
        conn.disconnect();
    }

    /**
     * Creates or overwrites a single coverage store by uploading its GeoTIFF raster data file
     *
     * @param inputStream       an InputStream to the GeoTIFF for updating
     * @param geoserverRestURL  the root URL to GeoServer
     * @param workspace         the name of the workspace containing the coverage stores
     * @param store             the name of the store to be created or overwritten
     * @param layer             the name of the new layer
     */
    public void putCoverageStoreSingleGeoTiff(InputStream inputStream, String geoserverRestURL, String workspace, String store, String layer) throws IOException {
        String putCoverageStoreURL = String.format("%s/workspaces/%s/coveragestores/%s/file.geotiff?coverageName=%s", geoserverRestURL, workspace, store, layer);
        LOGGER.info("Uploading GeoTIFF to the GeoServer coverage store: " + putCoverageStoreURL);

        byte[] geoTiffPayload = IOUtils.toByteArray(inputStream);
        LOGGER.info("GeoTIFF size (bytes): " + geoTiffPayload.length);

        URL url = new URL(putCoverageStoreURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(false);
        conn.setUseCaches(false);
        conn.setRequestProperty("Content-Type", "text/plain");
        //conn.setRequestProperty("charset", "utf-8");
        conn.setRequestMethod("PUT");

        DataOutputStream out = new DataOutputStream(conn.getOutputStream());
        out.write(geoTiffPayload);
        out.flush();
        out.close();

        int responseCode = conn.getResponseCode();
        String responseMessage = conn.getResponseMessage();
        LOGGER.info("GeoServer response: " + responseCode + ": " + responseMessage);
        conn.disconnect();
    }

    /**
     * Modify a layer's style
     *
     * @param geoserverRestURL  the root URL to GeoServer
     * @param workspace         the name of the workspace containing the coverage stores
     * @param layer             the name of the layer to modify
     * @param style             the default style to use for this layer
     */
    public void putLayerStyle(String geoserverRestURL, String workspace, String layer, String style) throws IOException {
        String payload = "<layer><defaultStyle><name>" + style + "</name></defaultStyle></layer>";
        String uploadStyleURL = String.format("%s/workspaces/%s/layers/%s", geoserverRestURL, workspace, layer);
        LOGGER.info("Updating layer style to '" + style + "':" + uploadStyleURL);

        URL url = new URL(uploadStyleURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(false);
        conn.setUseCaches(false);
        conn.setRequestProperty("Content-Type", "application/xml");
        //conn.setRequestProperty("charset", "utf-8");
        conn.setRequestMethod("PUT");

        DataOutputStream out = new DataOutputStream(conn.getOutputStream());
        out.writeBytes(payload);
        out.flush();
        out.close();

        int responseCode = conn.getResponseCode();
        String responseMessage = conn.getResponseMessage();
        LOGGER.info("GeoServer response: " + responseCode + ": " + responseMessage);
        conn.disconnect();
    }
}
