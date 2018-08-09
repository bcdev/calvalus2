package com.bc.calvalus.processing.fire.format;

/**
 */
public class LcRemappingS2 {

    public static final int INVALID_LC_CLASS = 0;

    public static boolean isInBurnableLcClass(int sourceLcClass) {
        switch (sourceLcClass) {
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6: {
                return true;
            }
            default:
                return false;
        }
    }

    public static int remap(int sourceLcClass) {
        switch (sourceLcClass) {
            case 10:
            case 11:
            case 12:
                return 10;
            case 20:
                return 20;
            case 30:
                return 30;
            case 40:
                return 40;
            case 50:
                return 50;
            case 60:
            case 61:
            case 62:
                return 60;
            case 70:
            case 71:
            case 72:
                return 70;
            case 80:
            case 81:
            case 82:
                return 80;
            case 90:
                return 90;
            case 100:
                return 100;
            case 110:
                return 110;
            case 120:
            case 121:
            case 122:
                return 120;
            case -126:
            case 130:
                return 130;
            case -116:
            case 140:
                return 140;
            case -106:
            case -104:
            case -103:
            case 150:
            case 152:
            case 153:
                return 150;
            case -96:
            case 160:
                return 160;
            case -86:
            case 170:
                return 170;
            case -76:
            case 180:
                return 180;
        }
        return INVALID_LC_CLASS;
    }
}
