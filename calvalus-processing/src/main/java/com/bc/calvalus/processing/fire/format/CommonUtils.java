package com.bc.calvalus.processing.fire.format;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CommonUtils {


    public static String getMerisTile(String merisBaPath) {
        int startIndex = merisBaPath.indexOf("BA_PIX_MER_") + "BA_PIX_MER_".length();
        return merisBaPath.substring(startIndex, startIndex + 6);
    }

    public static List<String> getMissingTiles(List<String> usedTiles) {
        List<String> missingTiles = new ArrayList<>();
        for (int v = 0; v <= 17; v++) {
            for (int h = 0; h <= 35; h++) {
                String tile = String.format("v%02dh%02d", v, h);
                if (!usedTiles.contains(tile)) {
                    missingTiles.add(tile);
                }
            }
        }
        return missingTiles;
    }

    public static FileStatus[] filterFileStatuses(FileStatus[] fileStatuses) {
        List<String> pathNames = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            pathNames.add(fileStatus.getPath().getName());
        }
        List<String> filteredPathNames = filterPathNames(pathNames);

        List<FileStatus> result = new ArrayList<>();
        for (FileStatus fileStatus : fileStatuses) {
            if (filteredPathNames.contains(fileStatus.getPath().getName())) {
                result.add(fileStatus);
            }
        }
        return result.toArray(new FileStatus[0]);
    }

    static List<String> filterPathNames(List<String> pathNames) {
        List<String> filteredPathNames = new ArrayList<>();
        for (String pathName : pathNames) {
            boolean isOld = pathName.contains("_v4.0.tif");
            if (isOld) {
                boolean isReplacedByNew = pathNames.contains(pathName.replace("_v4.0.tif", "_v4.1.tif"));
                if (!isReplacedByNew) {
                    filteredPathNames.add(pathName);
                }
            } else {
                filteredPathNames.add(pathName);
            }
        }

        return filteredPathNames;
    }

    public static SensorStrategy getStrategy(Configuration conf) {
        String sensor = conf.get("calvalus.sensor");
        if (sensor == null) {
            throw new IllegalArgumentException("calvalus.sensor must be set");
        }
        if (sensor.equals("MERIS")) {
            return new MerisStrategy();
        } else if (sensor.equals("S2")) {
            return new S2Strategy();
        } else if (sensor.equals("MODIS")) {
            return new ModisStrategy();
        } else if (sensor.equals("AVHRR")) {
            return new AvhrrStrategy();
        } else if (sensor.equals("OLCI")) {
            return new OlciStrategy();
        }
        throw new IllegalStateException("Missing configuration item 'calvalus.sensor'");
    }

    public static void fixH18Band(Product product, Product fixedProduct, String bandNameToFix) throws IOException {
        int width = product.getSceneRasterWidth();
        int height = product.getSceneRasterHeight();
        Band bandToFix = product.getBand(bandNameToFix);
        Band fixedBand = new Band(bandNameToFix, bandToFix.getDataType(), width, product.getSceneRasterHeight());
        fixedBand.setData(new ProductData.Short(width * height));
        fixedProduct.addBand(fixedBand);
        int[] pixels = new int[width];
        int[] fixedPixels = new int[width];
        for (int y = 0; y < product.getSceneRasterHeight(); y++) {
            bandToFix.readPixels(0, y, width, 1, pixels);
            System.arraycopy(pixels, 0, fixedPixels, pixels.length / 2, pixels.length / 2);
            System.arraycopy(pixels, pixels.length / 2, fixedPixels, 0, pixels.length / 2);
            fixedBand.setPixels(0, y, width, 1, fixedPixels);
        }
    }

    public static void fixH18BandUInt8(Product product, Product fixedProduct, String bandNameToFix) throws IOException {
        int width = product.getSceneRasterWidth();
        int height = product.getSceneRasterHeight();
        Band bandToFix = product.getBand(bandNameToFix);
        Band fixedBand = new Band(bandNameToFix, bandToFix.getDataType(), width, product.getSceneRasterHeight());
        fixedBand.setData(new ProductData.UByte(width * height));
        fixedProduct.addBand(fixedBand);
        int[] pixels = new int[width];
        int[] fixedPixels = new int[width];
        for (int y = 0; y < product.getSceneRasterHeight(); y++) {
            bandToFix.readPixels(0, y, width, 1, pixels);
            System.arraycopy(pixels, 0, fixedPixels, pixels.length / 2, pixels.length / 2);
            System.arraycopy(pixels, pixels.length / 2, fixedPixels, 0, pixels.length / 2);
            fixedBand.setPixels(0, y, width, 1, fixedPixels);
        }
    }

    public static void fixH18BandByte(Product product, Product fixedProduct, String bandNameToFix) throws IOException {
        int width = product.getSceneRasterWidth();
        int height = product.getSceneRasterHeight();
        Band bandToFix = product.getBand(bandNameToFix);
        Band fixedBand = new Band(bandNameToFix, bandToFix.getDataType(), width, product.getSceneRasterHeight());
        fixedBand.setData(new ProductData.UByte(width * height));
        fixedProduct.addBand(fixedBand);
        int[] pixels = new int[width];
        int[] fixedPixels = new int[width];
        for (int y = 0; y < product.getSceneRasterHeight(); y++) {
            bandToFix.readPixels(0, y, width, 1, pixels);
            System.arraycopy(pixels, 0, fixedPixels, pixels.length / 2, pixels.length / 2);
            System.arraycopy(pixels, pixels.length / 2, fixedPixels, 0, pixels.length / 2);
            fixedBand.setPixels(0, y, width, 1, fixedPixels);
        }
    }

    public static String lcYear(int year) {
        switch (year) {
            case 2000:
            case 2001:
            case 2002:
            case 2003:
            case 2004:
            case 2005:
            case 2006:
            case 2007:
                return "2000";
            case 2008:
            case 2009:
            case 2010:
            case 2011:
            case 2012:
                return "2005";
            case 2013:
            case 2014:
            case 2015:
            case 2016:
            case 2017:
            case 2018:
                return "2010";
        }
        throw new IllegalArgumentException("Illegal year: " + year);
    }

    public static float checkForBurnability(float sourceJd, int sourceLcClass, String sensor) {
        switch (sensor) {
            case "S2":
                if (!LcRemappingS2.isInBurnableLcClass(sourceLcClass)) {
                    return -2;
                } else {
                    return sourceJd;
                }
            case "MODIS":
            case "OLCI":
                if (!LcRemapping.isInBurnableLcClass(sourceLcClass)) {
                    return -2;
                } else {
                    return sourceJd;
                }
            default:
                throw new IllegalStateException("Unknown sensor '" + sensor + "'");
        }
    }

    public static File[] untar(File tarFile, String filterRegEx) {
        return untar(tarFile, filterRegEx, null);
    }

    public static File[] untar(File tarFile, String filterRegEx, List<String> newDirs) {
        List<File> result = new ArrayList<>();
        try (FileInputStream in = new FileInputStream(tarFile);
             GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(in);
             TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {

            TarArchiveEntry entry;

            while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    if (!new File(entry.getName()).exists()) {
                        boolean created = new File(entry.getName()).mkdirs();
                        if (!created) {
                            throw new IOException(String.format("Unable to create directory '%s' during extraction of contents of archive: '", entry.getName()));
                        }
                    }
                } else {
                    int count;
                    byte[] data = new byte[1024];
                    if (!entry.getName().matches(filterRegEx)) {
                        continue;
                    }
                    int lastIndex = entry.getName().lastIndexOf("/");
                    boolean created = new File(entry.getName().substring(0, lastIndex)).mkdirs();
                    if (created && newDirs != null) {
                        Collections.addAll(newDirs, entry.getName().substring(0, lastIndex).split("/"));
                    }
                    FileOutputStream fos = new FileOutputStream(entry.getName(), false);
                    try (BufferedOutputStream dest = new BufferedOutputStream(fos, 1024)) {
                        while ((count = tarIn.read(data, 0, 1024)) != -1) {
                            dest.write(data, 0, count);
                        }
                    }
                    result.add(new File(entry.getName()));
                }
            }

        } catch (IOException e) {
            throw new IllegalStateException("Unable to extract tar archive '" + tarFile + "'", e);
        }
        File[] untarredFiles = result.toArray(new File[0]);
        Arrays.sort(untarredFiles, Comparator.comparing(File::getName));
        return untarredFiles;
    }
}
