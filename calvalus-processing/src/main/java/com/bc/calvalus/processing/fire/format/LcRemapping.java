package com.bc.calvalus.processing.fire.format;

/**
 */
public class LcRemapping {

    public static final int INVALID_LC_CLASS = 0;
    public static final int LC_CLASSES_COUNT = 18;

    public static boolean isInBurnableLcClass(int sourceLcClass) {
        for (int lcClass = 0; lcClass < LC_CLASSES_COUNT; lcClass++) {
            if (isInLcClass(lcClass + 1, sourceLcClass)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isInLcClass(int lcClassNumber, int sourceLcClass) {
        switch (lcClassNumber) {
            case 1:
                return sourceLcClass == 10 || sourceLcClass == 11 || sourceLcClass == 12;
            case 2:
                return sourceLcClass == 20;
            case 3:
                return sourceLcClass == 30;
            case 4:
                return sourceLcClass == 40;
            case 5:
                return sourceLcClass == 50;
            case 6:
                return sourceLcClass == 60 || sourceLcClass == 61 || sourceLcClass == 62;
            case 7:
                return sourceLcClass == 70 || sourceLcClass == 71 || sourceLcClass == 72;
            case 8:
                return sourceLcClass == 80 || sourceLcClass == 81 || sourceLcClass == 82;
            case 9:
                return sourceLcClass == 90;
            case 10:
                return sourceLcClass == 100;
            case 11:
                return sourceLcClass == 110;
            case 12:
                return sourceLcClass == 120 || sourceLcClass == 121 || sourceLcClass == 122;
            case 13:
                return sourceLcClass == -126 || sourceLcClass == 130;
            case 14:
                return sourceLcClass == -116 || sourceLcClass == 140;
            case 15:
                return sourceLcClass == -106 || sourceLcClass == -104 || sourceLcClass == -103 || sourceLcClass == 150 || sourceLcClass == 152 || sourceLcClass == 153;
            case 16:
                return sourceLcClass == -96 || sourceLcClass == 160;
            case 17:
                return sourceLcClass == -86 || sourceLcClass == 170;
            case 18:
                return sourceLcClass == -76 || sourceLcClass == 180;
        }
        throw new IllegalArgumentException(String.format("Illegal target class: %d", lcClassNumber));
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
