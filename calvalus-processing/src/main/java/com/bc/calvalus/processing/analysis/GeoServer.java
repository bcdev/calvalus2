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
import com.bc.calvalus.processing.JobConfigNames;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.logging.Logger;

/**
 * GeoServer REST API handler
 *
 * @author Declan
 */
public class GeoServer {

    private static final Logger LOGGER = CalvalusLogger.getLogger();

    private final TaskAttemptContext context;
    private String basicAuth = "";

    public GeoServer(TaskAttemptContext context ) {
        this.context = context;
    }

    public void uploadImage(InputStream inputStream, String imageName) throws IOException {
        LOGGER.info(String.format("Uploading product image '%s.tiff' to GeoServer.", imageName));

        String geoserverRestURL = context.getConfiguration().get(JobConfigNames.CALVALUS_QUICKLOOK_UPLOAD_URL);
        if (geoserverRestURL == null || geoserverRestURL.isEmpty()) {
            throw new IllegalArgumentException("geoserverRestURL is empty");
        }
        geoserverRestURL = geoserverRestURL.trim();
        while (geoserverRestURL.endsWith("/")) {
            geoserverRestURL = geoserverRestURL.substring(0, geoserverRestURL.length() - 1);
        }

        String username = context.getConfiguration().get(JobConfigNames.CALVALUS_QUICKLOOK_UPLOAD_USERNAME);
        String password = context.getConfiguration().get(JobConfigNames.CALVALUS_QUICKLOOK_UPLOAD_PASSWORD);
        if (username != null && !username.isEmpty() && password != null) {
            String userpass = username + ":" + password;
            this.basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
        }

        String workspace = context.getConfiguration().get(JobConfigNames.CALVALUS_QUICKLOOK_UPLOAD_WORKSPACE);
        if (workspace == null || workspace.isEmpty()) {
            throw new IllegalArgumentException("workspace is empty");
        }

        String store = imageName;
        String layer = imageName;

        // other functionality for future (not required at present)
        //getCoverageStore(geoserverRestURL, workspace, store);
        //deleteCoverageStore(geoserverRestURL, workspace, store);
        //putLayerStyle(geoserverRestURL, workspace, layer, style);

        putCoverageStoreSingleGeoTiff(inputStream, geoserverRestURL, workspace, store, layer);
    }

    /**
     * Get a coverage store named {storeName} in the {workspace} workspace
     *
     * @param geoserverRestURL the root URL to GeoServer
     * @param workspace        the name of the workspace containing the coverage stores
     * @param store            the name of the store to be retrieved
     */
    private void getCoverageStore(String geoserverRestURL, String workspace, String store) throws IOException {
        String getCoverageStoreURL = String.format("%s/workspaces/%s/coveragestores/%s", geoserverRestURL, workspace, store);
        HttpURLConnection conn = null;
        try {
            LOGGER.info(String.format("Getting coverage store '%s' in workspace '%s' on GeoServer...", store, workspace));
            LOGGER.info(String.format("GeoServer URL: %s", getCoverageStoreURL));
            URL url = new URL(getCoverageStoreURL);
            conn = (HttpURLConnection) url.openConnection();
            if (!this.basicAuth.isEmpty()) {
                conn.setRequestProperty("Authorization", basicAuth);
            }
            conn.setDoOutput(true);
            conn.setInstanceFollowRedirects(false);
            conn.setUseCaches(false);
            conn.setRequestProperty("Content-Type", "application/xml");
            conn.setRequestProperty("charset", "utf-8");
            logResponse(conn);
            LOGGER.info(String.format("Finished getting coverage store '%s' in workspace '%s' on GeoServer", store, workspace));
        } catch (IOException e) {
            LOGGER.warning(String.format("GeoServer error: %s", e.getMessage()));
            logResponse(conn, true);
        } finally {
            if (conn != null)
                conn.disconnect();
        }
    }

    /**
     * Delete a coverage store named {storeName} in the {workspace} workspace
     *
     * @param geoserverRestURL the root URL to GeoServer
     * @param workspace        the name of the workspace containing the coverage stores
     * @param store            the name of the store to be deleted
     */
    private void deleteCoverageStore(String geoserverRestURL, String workspace, String store) throws IOException {
        String deleteCoverageStoreURL = String.format("%s/workspaces/%s/coveragestores/%s?recurse=true&purge=all", geoserverRestURL, workspace, store);
        HttpURLConnection conn = null;
        try {
            LOGGER.info(String.format("Deleting coverage store '%s' in workspace '%s' on GeoServer...", store, workspace));
            LOGGER.info(String.format("GeoServer URL: %s", deleteCoverageStoreURL));
            URL url = new URL(deleteCoverageStoreURL);
            conn = (HttpURLConnection) url.openConnection();
            if (!this.basicAuth.isEmpty()) {
                conn.setRequestProperty("Authorization", basicAuth);
            }
            conn.setDoOutput(true);
            conn.setInstanceFollowRedirects(false);
            conn.setUseCaches(false);
            conn.setRequestMethod("DELETE");
            logResponse(conn);
            LOGGER.info(String.format("Finished deleting coverage store '%s' in workspace '%s' on GeoServer", store, workspace));
        } catch (IOException e) {
            LOGGER.warning(String.format("GeoServer error: %s", e.getMessage()));
            logResponse(conn, true);
        } finally {
            if (conn != null)
                conn.disconnect();
        }
    }

    /**
     * Creates or overwrites a single coverage store by uploading its GeoTIFF raster data file
     *
     * @param inputStream      an InputStream to the GeoTIFF for upload
     * @param geoserverRestURL the root URL to GeoServer
     * @param workspace        the name of the workspace containing the coverage stores
     * @param store            the name of the store to be created or overwritten
     * @param layer            the name of the new layer
     */
    private void putCoverageStoreSingleGeoTiff(InputStream inputStream, String geoserverRestURL, String workspace, String store, String layer) throws IOException {
        String putCoverageStoreURL = String.format("%s/workspaces/%s/coveragestores/%s/file.geotiff?coverageName=%s", geoserverRestURL, workspace, store, layer);
        byte[] geoTiffPayload = IOUtils.toByteArray(inputStream);
        HttpURLConnection conn = null;
        DataOutputStream out = null;
        try {
            LOGGER.info(String.format("Uploading geoTIFF layer '%s' to coverage store '%s' in workspace '%s' on GeoServer...", layer, store, workspace));
            LOGGER.info(String.format("GeoServer URL: %s", putCoverageStoreURL));
            LOGGER.info(String.format("GeoTIFF size (bytes): %d", geoTiffPayload.length));
            URL url = new URL(putCoverageStoreURL);
            conn = (HttpURLConnection) url.openConnection();
            if (!this.basicAuth.isEmpty()) {
                conn.setRequestProperty("Authorization", basicAuth);
            }
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setInstanceFollowRedirects(false);
            conn.setUseCaches(false);
            conn.setRequestProperty("Content-Type", "text/plain");
            conn.setRequestMethod("PUT");
            out = new DataOutputStream(conn.getOutputStream());
            out.write(geoTiffPayload);
            out.flush();
            logResponse(conn);
            LOGGER.info(String.format("Finished uploading geoTIFF layer '%s' to coverage store '%s' in workspace '%s' on GeoServer", layer, store, workspace));
        } catch (IOException e) {
            LOGGER.warning(String.format("GeoServer error: %s", e.getMessage()));
            logResponse(conn, true);
        } finally {
            if (out != null)
                out.close();
            if (conn != null)
                conn.disconnect();
        }
    }

    /**
     * Modify a layer's style
     *
     * @param geoserverRestURL the root URL to GeoServer
     * @param workspace        the name of the workspace containing the coverage stores
     * @param layer            the name of the layer to modify
     * @param style            the default style to use for this layer
     */
    private void putLayerStyle(String geoserverRestURL, String workspace, String layer, String style) throws IOException {
        String payload = "<layer><defaultStyle><name>" + style + "</name></defaultStyle></layer>";
        String uploadStyleURL = String.format("%s/workspaces/%s/layers/%s", geoserverRestURL, workspace, layer);
        HttpURLConnection conn = null;
        DataOutputStream out = null;
        try {
            LOGGER.info(String.format("Updating layer '%s' to named style '%s' in workspace '%s' on GeoServer", layer, style, workspace));
            LOGGER.info(String.format("GeoServer URL: %s", uploadStyleURL));
            URL url = new URL(uploadStyleURL);
            conn = (HttpURLConnection) url.openConnection();
            if (!this.basicAuth.isEmpty()) {
                conn.setRequestProperty("Authorization", basicAuth);
            }
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setInstanceFollowRedirects(false);
            conn.setUseCaches(false);
            conn.setRequestProperty("Content-Type", "application/xml");
            conn.setRequestMethod("PUT");
            out = new DataOutputStream(conn.getOutputStream());
            out.writeBytes(payload);
            out.flush();
            logResponse(conn);
            LOGGER.info(String.format("Finished updating layer '%s' to named style '%s' in workspace '%s' on GeoServer", layer, style, workspace));
        } catch (IOException e) {
            LOGGER.warning(String.format("GeoServer error: %s", e.getMessage()));
            logResponse(conn, true);
        } finally {
            if (out != null)
                out.close();
            if (conn != null)
                conn.disconnect();
        }
    }

    private void logResponse(HttpURLConnection conn) throws IOException {
        logResponse(conn, false);
    }

    private void logResponse(HttpURLConnection conn, boolean exception) throws IOException {
        InputStream inputStream = null;
        try {
            int responseCode = conn.getResponseCode();
            if (exception) {
                inputStream = conn.getErrorStream();
            } else {
                inputStream = conn.getInputStream();
            }

            if (inputStream != null) {
                ByteArrayOutputStream response = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int length;
                while ((length = inputStream.read(buffer)) != -1) {
                    response.write(buffer, 0, length);
                }
                LOGGER.info(String.format("GeoServer HTTP response code: %d", responseCode));
                LOGGER.info(String.format("GeoServer HTTP response message: %s", response.toString()));
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}