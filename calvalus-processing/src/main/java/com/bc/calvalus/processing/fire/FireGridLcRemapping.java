package com.bc.calvalus.processing.fire;

/**
 */
class FireGridLcRemapping {

    static boolean isInLcClass(int targetLcClass, int sourceLcClass) {
        switch (targetLcClass) {
            case 1:
                return sourceLcClass == 10;
            case 2:
                return sourceLcClass == 20;
            case 3:
                return sourceLcClass == 30;
            case 4:
                return sourceLcClass == 40;
            case 5:
                return sourceLcClass == 50;
            case 6:
                return sourceLcClass == 60;
            case 7:
                return sourceLcClass == 70;
            case 8:
                return sourceLcClass == 80;
            case 9:
                return sourceLcClass == 90;
            case 10:
                return sourceLcClass == 100;
            case 11:
                return sourceLcClass == 110;
            case 12:
                return sourceLcClass == 120;
            case 13:
                return sourceLcClass == -126;
            case 14:
                return sourceLcClass == -116;
            case 15:
                return sourceLcClass == -106;
            case 16:
                return sourceLcClass == -96;
            case 17:
                return sourceLcClass == -86;
            case 18:
                return sourceLcClass == -76;
        }
        throw new IllegalArgumentException(String.format("Illegal single target class: %d", targetLcClass));
    }
}
