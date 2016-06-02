package com.bc.calvalus.processing.fire.format;

import org.apache.hadoop.fs.FileStatus;

import java.util.ArrayList;
import java.util.List;

public class CommonUtils {


    public static String getTile(String baPath) {
        int startIndex = baPath.indexOf("BA_PIX_MER_") + "BA_PIX_MER_".length();
        return baPath.substring(startIndex, startIndex + 6);
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
            boolean isOld = pathName.contains("/v1/");
            if (isOld) {
                boolean isReplacedByNew = pathNames.contains(pathName.replace("/v1/", "/").replace("_v4.0.tif", "_v4.1.tif"));
                if (!isReplacedByNew) {
                    filteredPathNames.add(pathName);
                }
            } else {
                filteredPathNames.add(pathName);
            }
        }

        return filteredPathNames;
    }

}
