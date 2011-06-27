package com.bc.calvalus.portal.server;

import com.bc.calvalus.portal.shared.DtoRegion;

import java.io.*;
import java.util.*;

/**
 * A location that stores regions.
 *
 * @author Norman
 * @since Calvalus 0.2
 */
public class RegionPersistence {

    private final String userName;

    public RegionPersistence(String userName) {
        this.userName = userName;
    }

    public DtoRegion[] loadRegions() throws IOException {
        Properties properties = loadDefaultRegions();
        Properties userProperties = loadUserRegions();
        properties.putAll(userProperties);

        ArrayList<DtoRegion> regions = new ArrayList<DtoRegion>();
        Set<String> regionNames = properties.stringPropertyNames();
        for (String regionName : regionNames) {
            String regionWKT = properties.getProperty(regionName);
            DtoRegion region = new DtoRegion(regionName, regionWKT);
            regions.add(region);
        }
        Collections.sort(regions, new Comparator<DtoRegion>() {
            @Override
            public int compare(DtoRegion o1, DtoRegion o2) {
                return o1.getName().compareToIgnoreCase(o2.getName());
            }
        });
        return regions.toArray(new DtoRegion[regions.size()]);
    }

    public void storeRegions(DtoRegion[] regions) throws IOException {
        Properties properties = getUserRegions(regions);
        File file = getUserRegionFile();
        file.getParentFile().mkdirs();
        FileWriter fileWriter = new FileWriter(file);
        try {
            properties.store(fileWriter, "Calvalus regions for user " + getUserName());
        } finally {
            fileWriter.close();
        }
    }

    private File getUserRegionFile() {
        return new File(System.getProperty("user.home"), ".calvalus/" + getUserName() + "-regions.properties");
    }

    private String getUserName() {
        return userName;
    }

    private Properties getUserRegions(DtoRegion[] regions) {
        Properties properties = new Properties();
        for (int i = 0; i < regions.length; i++) {
            DtoRegion region = regions[i];
            if (region.getName().startsWith("user.")) {
                System.out.println("storing region[" + i + "]:" + region.getName() + " = " + region.getGeometryWkt());
                properties.put(region.getName(), region.getGeometryWkt());
            }
        }
        return properties;
    }

    private Properties loadDefaultRegions() throws IOException {
        InputStream stream = getClass().getResourceAsStream("regions.properties");
        return loadProperties(new InputStreamReader(stream));
    }

    private Properties loadUserRegions() throws IOException {
        File userRegionFile = getUserRegionFile();
        if (userRegionFile.exists()) {
            return loadProperties(new FileReader(userRegionFile));
        } else {
            return new Properties();
        }
    }

    private Properties loadProperties(Reader stream) throws IOException {
        Properties properties = new Properties();
        try {
            properties.load(stream);
        } finally {
            stream.close();
        }
        return properties;
    }

}
