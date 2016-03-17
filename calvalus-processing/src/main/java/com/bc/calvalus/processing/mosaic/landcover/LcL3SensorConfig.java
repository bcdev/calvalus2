package com.bc.calvalus.processing.mosaic.landcover;

import com.bc.calvalus.processing.mosaic.MosaicConfig;
import org.esa.beam.binning.VariableContext;

import java.util.ArrayList;
import java.util.List;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public abstract class LcL3SensorConfig {

    static final String[] COUNTER_NAMES = { "clear_land", "clear_water", "clear_snow_ice", "cloud", "cloud_shadow" };

    static final LcL3SensorConfig LC_L3_FR_CONFIG = new LcL3FrConfig();
    static final LcL3SensorConfig LC_L3_RR_CONFIG = new LcL3RrConfig();
    static final LcL3SensorConfig LC_L3_SPOT_CONFIG = new LcL3SpotConfig();
    static final LcL3SensorConfig LC_L3_PROBA_CONFIG = new LcL3ProbaConfig();
    static final LcL3SensorConfig LC_L3_AVHRR_CONFIG = new LcL3AvhrrConfig();

    public static LcL3SensorConfig create(String sensor, String spatialResolution) {
        if (LC_L3_FR_CONFIG.getSensorName().equals(sensor) && LC_L3_FR_CONFIG.getGroundResolution().equals(spatialResolution)) {
            return LC_L3_FR_CONFIG;
        } else if (LC_L3_RR_CONFIG.getSensorName().equals(sensor) && LC_L3_RR_CONFIG.getGroundResolution().equals(spatialResolution)) {
            return LC_L3_RR_CONFIG;
        } else if (LC_L3_SPOT_CONFIG.getSensorName().equals(sensor) && LC_L3_SPOT_CONFIG.getGroundResolution().equals(spatialResolution)) {
            return LC_L3_SPOT_CONFIG;
        } else if (LC_L3_PROBA_CONFIG.getSensorName().equals(sensor) && LC_L3_PROBA_CONFIG.getGroundResolution().equals(spatialResolution)) {
            return LC_L3_PROBA_CONFIG;
        } else if (LC_L3_AVHRR_CONFIG.getSensorName().equals(sensor) && LC_L3_AVHRR_CONFIG.getGroundResolution().equals(spatialResolution)) {
            return LC_L3_AVHRR_CONFIG;
        } else {
            throw new IllegalArgumentException("unknown sensor resolution combination " + sensor + " " + spatialResolution);
        }
    }

    public static LcL3SensorConfig create(String resolution) {
        if ("FR".equals(resolution)) {
            return LC_L3_FR_CONFIG;
        } else if ("RR".equals(resolution)) {
            return LC_L3_RR_CONFIG;
        } else if ("SPOT".equals(resolution)) {
            return LC_L3_SPOT_CONFIG;
        } else if ("PROBA".equals(resolution)) {
            return LC_L3_PROBA_CONFIG;
        } else if ("HRPT".equals(resolution)) {
            return LC_L3_AVHRR_CONFIG;
        } else {
            throw new IllegalArgumentException("unknown resolution " + resolution);
        }
    }

    public abstract int getMosaicTileSize();

    public abstract String getGroundResolution();

    public abstract String getSensorName();

    public abstract String getPlatformName();

    public abstract String[] getBandNames();

    public abstract float[] getWavelengths();

    public abstract MosaicConfig getCloudMosaicConfig(String temporalCloudBandName, String asLandText, int borderWidth);

    public abstract MosaicConfig getMainMosaicConfig(String netCDF4, int borderWidth);

    public abstract String getTemporalCloudBandName();

    public abstract int getTemporalCloudBandIndex();

    public abstract float getTemporalCloudFilterThreshold();

    public abstract int[] createVariableIndexes(VariableContext varCtx);

    public abstract String[] createOutputFeatureNames();

    static int getVariableIndex(VariableContext varCtx, String varName) {
        int varIndex = varCtx.getVariableIndex(varName);
        if (varIndex < 0) {
            throw new IllegalArgumentException(String.format("varIndex < 0 for varName='%s'", varName));
        }
        return varIndex;
    }

    public abstract int getL2BandIndex(String srBandName);

    public abstract List<String> getMeanBandNames();

    public abstract List<String> getUncertaintyBandNames();

    public abstract boolean isUncertaintiesAreSquares();


    public static abstract class LcL3MerisConfig extends LcL3SensorConfig {

        static final String[] BANDNAMES = new String[] {
                "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"
        };
        static final float[] WAVELENGTH = new float[] {
            412.691f, 442.55902f, 489.88202f, 509.81903f, 559.69403f,
            619.601f, 664.57306f, 680.82104f, 708.32904f, 753.37103f,
            761.50806f, 778.40906f, 864.87604f, 884.94403f, 900.00006f
        };

        @Override
        public String getSensorName() {
            return "MERIS";
        }

        @Override
        public String getPlatformName() {
            return "ENVISAT";
        }

        @Override
        public String[] getBandNames() {
            return BANDNAMES;
        }

        @Override
        public float[] getWavelengths() {
            return WAVELENGTH;
        }

        public String getTemporalCloudBandName() {
            return "sdr_8";
        }

        @Override
        public int getTemporalCloudBandIndex() {
            return 8;
        }

        public float getTemporalCloudFilterThreshold() {
            return 0.075f;
        }

        public MosaicConfig getCloudMosaicConfig(String sdrBandName, String asLandText, int borderWidth) {
            String maskExpr;
            if (asLandText != null) {
                StatusRemapper statusRemapper = StatusRemapper.create(asLandText);
                int[] statusesToLand = statusRemapper.getStatusesToLand();
                StringBuilder sb = new StringBuilder();
                for (int i : statusesToLand) {
                    sb.append(" or status == ");
                    sb.append(i);
                }
                maskExpr = "(status == 1 " + sb.toString() + ") and not nan(" + sdrBandName + ")";
            } else {
                maskExpr = "status == 1 and not nan(" + sdrBandName + ")";
            }
            String[] varNames = new String[]{"status", sdrBandName, "ndvi"};
            String type = LcSDR8MosaicAlgorithm.class.getName();

            return new MosaicConfig(type, maskExpr, varNames);
        }

        public MosaicConfig getMainMosaicConfig(String outputFormat, int borderWidth) {
            String maskExpr;
            String[] varNames;
            // exclude invalid and deep water pixels
            maskExpr = "(status == 1 or (status == 2 and not nan(sdr_1)) or status == 3 or ((status >= 4) and dem_alt > -100))";
            // TODO harmonise names regarding error, uncertainty, sigma
            varNames = new String[]{
                    "status",
                    "sdr_1", "sdr_2", "sdr_3", "sdr_4", "sdr_5",
                    "sdr_6", "sdr_7", "sdr_8", "sdr_9", "sdr_10",
                    "sdr_11", "sdr_12", "sdr_13", "sdr_14", "sdr_15",
                    "ndvi",
                    "sdr_error_1", "sdr_error_2", "sdr_error_3", "sdr_error_4", "sdr_error_5",
                    "sdr_error_6", "sdr_error_7", "sdr_error_8", "sdr_error_9", "sdr_error_10",
                    "sdr_error_11", "sdr_error_12", "sdr_error_13", "sdr_error_14", "sdr_error_15",
            };

            String type = "NetCDF4-LC".equals(outputFormat)
                    ? LcL3Nc4MosaicAlgorithm.class.getName()
                    : LCMosaicAlgorithm.class.getName();

            return new MosaicConfig(type, maskExpr, varNames);
        }

        public int[] createVariableIndexes(VariableContext varCtx) {
            int[] varIndexes = new int[2 + BANDNAMES.length * 2];
            int j = 0;
            varIndexes[j++] = getVariableIndex(varCtx, "status");
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = Integer.toString(i + 1);
                varIndexes[j++] = getVariableIndex(varCtx, "sdr_" + bandSuffix);
            }
            varIndexes[j++] = getVariableIndex(varCtx, "ndvi");
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = Integer.toString(i + 1);
                varIndexes[j++] = getVariableIndex(varCtx, "sdr_error_" + bandSuffix);
            }
            return varIndexes;
        }

        public String[] createOutputFeatureNames() {
            String[] featureNames = new String[2 + COUNTER_NAMES.length + (BANDNAMES.length * 2)];
            int j = 0;
            featureNames[j++] = "status";
            for (String counter : COUNTER_NAMES) {
                featureNames[j++] = counter + "_count";
            }
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = Integer.toString(i + 1);
                featureNames[j++] = "sr_" + bandSuffix + "_mean";
            }
            featureNames[j++] = "ndvi_mean";
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = Integer.toString(i + 1);
                featureNames[j++] = "sr_" + bandSuffix + "_sigma";
            }
            return featureNames;
        }

        @Override
        public List<String> getMeanBandNames() {
            ArrayList<String> names = new ArrayList<String>(BANDNAMES.length);
            for (String name : BANDNAMES) {
                names.add("sr_" + name + "_mean");
            }
            return names;
        }

        @Override
        public List<String> getUncertaintyBandNames() {
            ArrayList<String> names = new ArrayList<String>(BANDNAMES.length);
            for (String name : BANDNAMES) {
                names.add("sr_" + name + "_uncertainty");
            }
            return names;
        }

        @Override
        public boolean isUncertaintiesAreSquares() {
            return true;
        }

        @Override
        public int getL2BandIndex(String srBandName) {
            final String name = srBandName.substring("sr_".length(), srBandName.length() - "_mean".length());
            for (int j=0; j<BANDNAMES.length; ++j) {
                if (BANDNAMES[j].equals(name)) {
                    return j+1;
                }
            }
            return -1;
        }
    }


    public static class LcL3FrConfig extends LcL3MerisConfig {
        @Override
        public int getMosaicTileSize() {
            return 360;
        }
        @Override
        public String getGroundResolution() {
            return "300m";
        }
    }


    public static class LcL3RrConfig extends LcL3MerisConfig {
        @Override
        public int getMosaicTileSize() {
            return 90;
        }
        @Override
        public String getGroundResolution() {
            return "1000m";
        }
    }


    public static class LcL3SpotConfig extends LcL3SensorConfig {

        static final String[] BANDNAMES = new String[] {
                "B0", "B2", "B3", "MIR"
        };
        static final float[] WAVELENGTH = new float[] {
            450f, 645f, 835f, 1665f
        };

        @Override
        public int getMosaicTileSize() {
            return 90;
        }
        @Override
        public String getGroundResolution() {
            return "1000m";
        }

        @Override
        public String getSensorName() {
            return "VEGETATION";
        }

        @Override
        public String getPlatformName() {
            return "SPOT";
        }

        @Override
        public String[] getBandNames() {
            return BANDNAMES;
        }

        @Override
        public float[] getWavelengths() {
            return WAVELENGTH;
        }

        public String getTemporalCloudBandName() {
            return "sdr_B3";
        }

        @Override
        public int getTemporalCloudBandIndex() {
            return 3;
        }

        public float getTemporalCloudFilterThreshold() {
            return 0.075f;
        }

        public MosaicConfig getCloudMosaicConfig(String sdrBandName, String asLandText, int borderWidth) {
            String maskExpr;
            if (asLandText != null) {
                StatusRemapper statusRemapper = StatusRemapper.create(asLandText);
                int[] statusesToLand = statusRemapper.getStatusesToLand();
                StringBuilder sb = new StringBuilder();
                for (int i : statusesToLand) {
                    sb.append(" or status == ");
                    sb.append(i);
                }
                maskExpr = "(status == 1 " + sb.toString() + ") and not nan(" + sdrBandName + ")";
            } else {
                maskExpr = "status == 1 and not nan(" + sdrBandName + ")";
            }
            String[] varNames = new String[]{"status", sdrBandName, "ndvi"};
            String type = LcSDR8MosaicAlgorithm.class.getName();

            return new MosaicConfig(type, maskExpr, varNames);
        }

        public MosaicConfig getMainMosaicConfig(String outputFormat, int borderWidth) {
            String maskExpr;
            String[] varNames;
            // exclude invalid
            maskExpr = "(status == 1 or (status == 2 and not nan(sdr_B0)) or (status >= 3))";

            varNames = new String[]{
                    "status",
                    "sdr_B0", "sdr_B2", "sdr_B3", "sdr_MIR",
                    "ndvi",
                    "sdr_error_B0", "sdr_error_B2", "sdr_error_B3", "sdr_error_MIR"
            };

            String type = "NetCDF4-LC".equals(outputFormat)
                    ? LcL3Nc4MosaicAlgorithm.class.getName()
                    : LCMosaicAlgorithm.class.getName();

            return new MosaicConfig(type, maskExpr, varNames);
        }

        public int[] createVariableIndexes(VariableContext varCtx) {
            int[] varIndexes = new int[2 + BANDNAMES.length * 2];
            int j = 0;
            varIndexes[j++] = getVariableIndex(varCtx, "status");
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = BANDNAMES[i];
                varIndexes[j++] = getVariableIndex(varCtx, "sdr_" + bandSuffix);
            }
            varIndexes[j++] = getVariableIndex(varCtx, "ndvi");
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = BANDNAMES[i];
                varIndexes[j++] = getVariableIndex(varCtx, "sdr_error_" + bandSuffix);
            }
            return varIndexes;
        }

        public String[] createOutputFeatureNames() {
            String[] featureNames = new String[2 + COUNTER_NAMES.length + BANDNAMES.length * 2];
            int j = 0;
            featureNames[j++] = "status";
            for (String counter : COUNTER_NAMES) {
                featureNames[j++] = counter + "_count";
            }
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = BANDNAMES[i];
                featureNames[j++] = "sr_" + bandSuffix + "_mean";
            }
            featureNames[j++] = "ndvi_mean";
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = BANDNAMES[i];
                featureNames[j++] = "sr_" + bandSuffix + "_sigma";
            }
            return featureNames;
        }

        @Override
        public List<String> getMeanBandNames() {
            ArrayList<String> names = new ArrayList<String>(BANDNAMES.length);
            for (String name : BANDNAMES) {
                names.add("sr_" + name + "_mean");
            }
            return names;
        }

        @Override
        public List<String> getUncertaintyBandNames() {
            ArrayList<String> names = new ArrayList<String>(BANDNAMES.length);
            for (String name : BANDNAMES) {
                names.add("sr_" + name + "_uncertainty");
            }
            return names;
        }

        @Override
        public boolean isUncertaintiesAreSquares() {
            return true;
        }

        @Override
        public int getL2BandIndex(String srBandName) {
            final String name = srBandName.substring("sr_".length(), srBandName.length() - "_mean".length());
            for (int j=0; j<BANDNAMES.length; ++j) {
                if (BANDNAMES[j].equals(name)) {
                    return j+1;
                }
            }
            return -1;
        }
    }

    public static class LcL3ProbaConfig extends LcL3SensorConfig {

        static final String[] BANDNAMES = new String[] {
                "BLUE", "RED", "NIR", "SWIR"
        };
        static final float[] WAVELENGTH = new float[] {
            450f, 645f, 835f, 1665f
        };

        @Override
        public int getMosaicTileSize() {
            return 360;
        }
        @Override
        public String getGroundResolution() {
            return "300m";
        }

        @Override
        public String getSensorName() {
            return "VEGETATION";
        }

        @Override
        public String getPlatformName() {
            return "PROBA";
        }

        @Override
        public String[] getBandNames() {
            return BANDNAMES;
        }

        @Override
        public float[] getWavelengths() {
            return WAVELENGTH;
        }

        public String getTemporalCloudBandName() {
            return "sdr_NIR";
        }

        @Override
        public int getTemporalCloudBandIndex() {
            return 3;
        }

        public float getTemporalCloudFilterThreshold() {
            return 0.075f;
        }

        public MosaicConfig getCloudMosaicConfig(String sdrBandName, String asLandText, int borderWidth) {
            String maskExpr;
            if (asLandText != null) {
                StatusRemapper statusRemapper = StatusRemapper.create(asLandText);
                int[] statusesToLand = statusRemapper.getStatusesToLand();
                StringBuilder sb = new StringBuilder();
                for (int i : statusesToLand) {
                    sb.append(" or status == ");
                    sb.append(i);
                }
                maskExpr = "(status == 1 " + sb.toString() + ") and not nan(" + sdrBandName + ")";
            } else {
                maskExpr = "status == 1 and not nan(" + sdrBandName + ")";
            }
            String[] varNames = new String[]{"status", sdrBandName, "ndvi"};
            String type = LcSDR8MosaicAlgorithm.class.getName();

            return new MosaicConfig(type, maskExpr, varNames);
        }

        public MosaicConfig getMainMosaicConfig(String outputFormat, int borderWidth) {
            String maskExpr;
            String[] varNames;
            // exclude invalid
            maskExpr = "(status == 1 or (status == 2 and not nan(sdr_BLUE)) or (status >= 3))";

            varNames = new String[]{
                    "status",
                    "sdr_BLUE", "sdr_RED", "sdr_NIR", "sdr_SWIR",
                    "ndvi",
                    "sdr_error_BLUE", "sdr_error_RED", "sdr_error_NIR", "sdr_error_SWIR"
            };

            String type = "NetCDF4-LC".equals(outputFormat)
                    ? LcL3Nc4MosaicAlgorithm.class.getName()
                    : LCMosaicAlgorithm.class.getName();

            return new MosaicConfig(type, maskExpr, varNames);
        }

        public int[] createVariableIndexes(VariableContext varCtx) {
            int[] varIndexes = new int[2 + BANDNAMES.length * 2];
            int j = 0;
            varIndexes[j++] = getVariableIndex(varCtx, "status");
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = BANDNAMES[i];
                varIndexes[j++] = getVariableIndex(varCtx, "sdr_" + bandSuffix);
            }
            varIndexes[j++] = getVariableIndex(varCtx, "ndvi");
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = BANDNAMES[i];
                varIndexes[j++] = getVariableIndex(varCtx, "sdr_error_" + bandSuffix);
            }
            return varIndexes;
        }

        public String[] createOutputFeatureNames() {
            String[] featureNames = new String[2 + COUNTER_NAMES.length + BANDNAMES.length * 2];
            int j = 0;
            featureNames[j++] = "status";
            for (String counter : COUNTER_NAMES) {
                featureNames[j++] = counter + "_count";
            }
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = BANDNAMES[i];
                featureNames[j++] = "sr_" + (i+1) + "_mean";
            }
            featureNames[j++] = "ndvi_mean";
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = BANDNAMES[i];
                featureNames[j++] = "sr_" + (i+1) + "_uncertainty";
            }
            return featureNames;
        }

        @Override
        public List<String> getMeanBandNames() {
            ArrayList<String> names = new ArrayList<String>(BANDNAMES.length);
            for (int i = 0; i < BANDNAMES.length; ++i) {
                names.add("sr_" + (i+1) + "_mean");
            }
            return names;
        }

        @Override
        public List<String> getUncertaintyBandNames() {
            ArrayList<String> names = new ArrayList<String>(BANDNAMES.length);
            for (int i = 0; i < BANDNAMES.length; ++i) {
                names.add("sr_" + (i+1) + "_uncertainty");
            }
            return names;
        }

        @Override
        public boolean isUncertaintiesAreSquares() {
            return false;
        }

        @Override
        public int getL2BandIndex(String srBandName) {
            final String name = srBandName.substring("sr_".length(), srBandName.length() - "_mean".length());
            return Integer.parseInt(name);
        }
    }


    public static class LcL3AvhrrConfig extends LcL3SensorConfig {

        static final String[] BANDNAMES = new String[] {
                "refl_1_ac", "refl_2_ac", "bt_3", "bt_4", "bt_5"
        };
        static final float[] WAVELENGTH = new float[] {
            630f, 912f, 3740f, 10800f, 12000f
        };

        @Override
        public int getMosaicTileSize() {
            return 90;
        }
        @Override
        public String getGroundResolution() {
            return "1000m";
        }

        @Override
        public String getSensorName() {
            return "AVHRR";
        }

        @Override
        public String getPlatformName() {
            return "NOAA";
        }

        @Override
        public String[] getBandNames() {
            return BANDNAMES;
        }

        @Override
        public float[] getWavelengths() {
            return WAVELENGTH;
        }

        public String getTemporalCloudBandName() {
            return "refl_2_ac";
        }

        @Override
        public int getTemporalCloudBandIndex() {
            return 2;
        }

        public float getTemporalCloudFilterThreshold() {
            return 0.075f;
        }

        public MosaicConfig getCloudMosaicConfig(String sdrBandName, String asLandText, int borderWidth) {
            String maskExpr;
            if (asLandText != null) {
                StatusRemapper statusRemapper = StatusRemapper.create(asLandText);
                int[] statusesToLand = statusRemapper.getStatusesToLand();
                StringBuilder sb = new StringBuilder();
                for (int i : statusesToLand) {
                    sb.append(" or status == ");
                    sb.append(i);
                }
                maskExpr = "((status & 3 != 0" + sb.toString() + ") and not nan(" + sdrBandName + ")";
            } else {
                maskExpr = "status & 3 != 0 and not nan(" + sdrBandName + ")";
            }
            String[] varNames = new String[]{sdrBandName};
            final String[] virtualVariableName = {
                    "status",
                    "ndvi"
            };
            final String[] virtualVariableExpr = {
                    //"(swath_x < " + borderWidth + " || swath_x >= " + (2048 - borderWidth) + " || pixel_classif_flags == 0 || pixel_classif_flags.F_INVALID) ? 0 : pixel_classif_flags.F_CLOUD ? 4 : pixel_classif_flags.F_CLOUD_SHADOW ? 5 : pixel_classif_flags.F_SNOW_ICE ? 3 : pixel_classif_flags.F_LAND ? 1 : 2",
                    "(" + (borderWidth > 0 ? ("swath_x < " + borderWidth + " || swath_x >= " + (2048 - borderWidth) + " || sza > 70 || ") : "") +
                            "(pixel_classif_flags & 1 != 0)) ? 0 : " +
                            "(pixel_classif_flags & (2+16) != 0) ? 4 : " +
                            "nan(refl_2_ac) ? 0 : " +
                            "(pixel_classif_flags & 32 != 0) ? 5 : " +
                            "(pixel_classif_flags & 64 != 0) ? 3 : " +
                            "(pixel_classif_flags & 1024 != 0) ? 1 : 2",
                    "(refl_2_ac - refl_1_ac) / (refl_2_ac + refl_1_ac)"
            };

            String type = LcSDR8MosaicAlgorithm.class.getName();

            return new MosaicConfig(type, maskExpr, varNames, virtualVariableName, virtualVariableExpr);
        }

        public MosaicConfig getMainMosaicConfig(String outputFormat, int borderWidth) {
            String maskExpr;
            String[] varNames;
            // exclude invalid
            maskExpr = "(status == 1 or (status == 2 and not nan(refl_2_ac)) or (status >= 3))";

            varNames = new String[]{
                    "refl_1_ac", "refl_2_ac", "bt_3", "bt_4", "bt_5",
                    "refl_1_ac_uncertainty", "refl_2_ac_uncertainty"
            };

            // add status band as virtual band from pixel_classif_flags
            final String[] virtualVariableName = {
                    "status",
                    "ndvi"
            };

            //pixel_classif_flags:flag_meanings = "F_INVALID F_CLOUD F_CLOUD_AMBIGUOUS F_CLOUD_SURE F_CLOUD_BUFFER F_CLOUD_SHADOW F_SNOW_ICE F_MIXED_PIXEL F_GLINT_RISK F_COASTLINE F_LAND F_REFL1_ABOVE_THRESH F_REFL2_ABOVE_THRESH F_RATIO_REFL21_ABOVE_THRESH F_RATIO_REFL31_ABOVE_THRESH F_BT4_ABOVE_THRESH F_BT5_ABOVE_THRESH" ;
            //pixel_classif_flags:flag_masks = 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 512s, 1024s, 2048s, 4096s, 8192s, 16384s, -32768s, 0s ;

            final String[] virtualVariableExpr = {
                    //"(swath_x < " + borderWidth + " || swath_x >= " + (2048 - borderWidth) + " || pixel_classif_flags == 0 || pixel_classif_flags.F_INVALID) ? 0 : pixel_classif_flags.F_CLOUD ? 4 : pixel_classif_flags.F_CLOUD_SHADOW ? 5 : pixel_classif_flags.F_SNOW_ICE ? 3 : pixel_classif_flags.F_LAND ? 1 : 2",
                    "(" + (borderWidth > 0 ? ("swath_x < " + borderWidth + " || swath_x >= " + (2048 - borderWidth) + " || sza > 70 || ") : "") +
                            "(pixel_classif_flags & 1 != 0)) ? 0 : " +
                            "(pixel_classif_flags & (2+16) != 0) ? 4 : " +
                            "nan(refl_2_ac) ? 0 : " +
                            "(pixel_classif_flags & 32 != 0) ? 5 : " +
                            "(pixel_classif_flags & 64 != 0) ? 3 : " +
                            "(pixel_classif_flags & 1024 != 0) ? 1 : 2",
                    "(refl_2_ac - refl_1_ac) / (refl_2_ac + refl_1_ac)"
            };

            String type = "NetCDF4-LC".equals(outputFormat)
                    ? LcL3Nc4MosaicAlgorithm.class.getName()
                    : LCMosaicAlgorithm.class.getName();

            return new MosaicConfig(type, maskExpr, varNames, virtualVariableName, virtualVariableExpr);
        }

        public int[] createVariableIndexes(VariableContext varCtx) {
            int[] varIndexes = new int[2 + BANDNAMES.length + 2];
            int j = 0;
            varIndexes[j++] = getVariableIndex(varCtx, "status");
            for (int i = 0; i < BANDNAMES.length; i++) {
                String bandSuffix = BANDNAMES[i];
                varIndexes[j++] = getVariableIndex(varCtx, bandSuffix);
            }
            varIndexes[j++] = getVariableIndex(varCtx, "ndvi");
            for (int i = 0; i < 2; i++) {
                String bandSuffix = BANDNAMES[i];
                varIndexes[j++] = getVariableIndex(varCtx, bandSuffix + "_uncertainty");
            }
            return varIndexes;
        }

        public String[] createOutputFeatureNames() {
            String[] featureNames = new String[2 + COUNTER_NAMES.length + BANDNAMES.length + 2];
            int j = 0;
            featureNames[j++] = "status";
            for (String counter : COUNTER_NAMES) {
                featureNames[j++] = counter + "_count";
            }
            for (int i = 0; i < BANDNAMES.length; i++) {
                featureNames[j++] = outputBandNameOf(BANDNAMES[i]) + "_mean";
            }
            featureNames[j++] = "ndvi_mean";
            for (int i = 0; i < 2; i++) {
                featureNames[j++] = outputBandNameOf(BANDNAMES[i]) + "_uncertainty";
            }
            return featureNames;
        }

        @Override
        public List<String> getMeanBandNames() {
            ArrayList<String> names = new ArrayList<String>(BANDNAMES.length);
            for (String name : BANDNAMES) {
                names.add(outputBandNameOf(name) + "_mean");
            }
            return names;
        }

        @Override
        public List<String> getUncertaintyBandNames() {
            ArrayList<String> names = new ArrayList<String>(0);
            for (int i = 0; i < 2; i++) {
                names.add(outputBandNameOf(BANDNAMES[i]) + "_uncertainty");
            }
            return names;
        }



        @Override
        public boolean isUncertaintiesAreSquares() {
            return false;
        }

        @Override
        public int getL2BandIndex(String srBandName) {
            final String bandname = inputBandNameOf(srBandName);
            final String name = bandname.substring(0, bandname.length() - "_mean".length());
            for (int j=0; j<BANDNAMES.length; ++j) {
                if (BANDNAMES[j].equals(name)) {
                    return j+1;
                }
            }
            return -1;
        }

        // refl_2_ac gets sr_2
        private String outputBandNameOf(String bandname) {
            if (bandname.startsWith("refl_")) {
                return "sr_" + bandname.substring(5,6) + bandname.substring(9);
            } else {
                return bandname;
            }
        }

        // sr_2 gets refl_2_ac
        private String inputBandNameOf(String srBandName) {
            if (srBandName.startsWith("sr_")) {
                return "refl_" + srBandName.substring(3,4) + "_ac" + srBandName.substring(4);
            } else {
                return srBandName;
            }
        }
    }
}
