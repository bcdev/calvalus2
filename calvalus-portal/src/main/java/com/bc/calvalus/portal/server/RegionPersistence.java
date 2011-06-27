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
        Properties regionProperties = loadDefaultRegions();
        Properties userRegionProperties = loadUserRegions();
        regionProperties.putAll(userRegionProperties);

        ArrayList<DtoRegion> regions = new ArrayList<DtoRegion>();
        Set<String> regionNames = regionProperties.stringPropertyNames();
        for (String regionFullName : regionNames) {
            int dotPos = regionFullName.indexOf('.');
            String regionName =  dotPos >= 0 ? regionFullName.substring(dotPos + 1) : regionFullName;
            String regionCategory =  dotPos >= 0 ? regionFullName.substring(0, dotPos) : "";
            String regionWKT = regionProperties.getProperty(regionFullName);
            DtoRegion region = new DtoRegion(regionName, regionCategory, regionWKT);
            regions.add(region);
        }
        Collections.sort(regions, new Comparator<DtoRegion>() {
            @Override
            public int compare(DtoRegion o1, DtoRegion o2) {
                int i = o1.getCategory().compareToIgnoreCase(o2.getCategory());
                return i != 0 ? i : o1.getName().compareToIgnoreCase(o2.getName());
            }
        });
        return regions.toArray(new DtoRegion[regions.size()]);
    }

    public void storeRegions(DtoRegion[] regions) throws IOException {
        Properties userRegions = getUserRegions(regions);
        File file = getUserRegionFile();
        file.getParentFile().mkdirs();
        FileWriter fileWriter = new FileWriter(file);
        try {
            userRegions.store(fileWriter, "Calvalus regions for user " + getUserName());
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
        Properties userRegions = new Properties();
        for (int i = 0; i < regions.length; i++) {
            DtoRegion region = regions[i];
            if (region.getCategory().equalsIgnoreCase("user")) {
                String fullName = region.getCategory() + "." + region.getName();
                System.out.println("storing region " + fullName + " = " + region.getGeometryWkt());
                userRegions.put(fullName, region.getGeometryWkt());
            }
        }
        return userRegions;
    }

    private Properties loadDefaultRegions() throws IOException {
        InputStream stream = getClass().getResourceAsStream("regions.properties");
        return loadRegions(new InputStreamReader(stream));
    }

    private Properties loadUserRegions() throws IOException {
        File userRegionFile = getUserRegionFile();
        if (userRegionFile.exists()) {
            return loadRegions(new FileReader(userRegionFile));
        } else {
            return new Properties();
        }
    }

    private Properties loadRegions(Reader stream) throws IOException {
        Properties regions = new Properties();
        try {
            regions.load(stream);
        } finally {
            stream.close();
        }
        return regions;
    }

}
