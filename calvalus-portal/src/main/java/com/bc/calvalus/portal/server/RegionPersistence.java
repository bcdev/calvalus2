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
            String[] split = regionFullName.split("\\.");
            String regionName = split[split.length - 1];
            String[] regionPath = Arrays.copyOf(split, split.length - 1);
            String regionWKT = regionProperties.getProperty(regionFullName);
            DtoRegion region = new DtoRegion(regionName, regionPath, regionWKT);
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
        for (DtoRegion region : regions) {
            if (region.isUserRegion()) {
                String fullName = region.getQualifiedName();
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
