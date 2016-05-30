package com.bc.calvalus.processing.fire.format;

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
}
