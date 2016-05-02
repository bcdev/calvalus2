package com.bc.calvalus.processing.fire;

/**
 */
class FireGridLcRemapping {

    static boolean isSingleTarget(int lcClass) {
        return lcClass == 1 || lcClass == 2 || lcClass == 9 || lcClass == 12 || lcClass == 13 || lcClass == 14 || lcClass == 16 || lcClass == 17 || lcClass == 18;
    }

    static boolean isInSingleLcClass(int targetLcClass, int sourceLcClass) {
        switch (targetLcClass) {
            case 1:
                return sourceLcClass == 10;
            case 2:
                return sourceLcClass == 20;
            case 9:
                return sourceLcClass == 90;
            case 12:
                return sourceLcClass == 120;
            case 13:
                return sourceLcClass == -126;
            case 14:
                return sourceLcClass == -116;
            case 16:
                return sourceLcClass == -96;
            case 17:
                return sourceLcClass == -86;
            case 18:
                return sourceLcClass == -76;
        }
        throw new IllegalArgumentException(String.format("Illegal single target class: %d", targetLcClass));
    }

    static boolean isPartOfMultiClass(int lcClass, int pixel) {
        switch (lcClass) {
            case 3:
                return pixel == 30 || pixel == 40;
            case 4:
                return pixel == 40 || pixel == 30;
            case 5:
                return pixel == 50;
            case 6:
                return pixel == 60;
            case 7:
                return pixel == 70;
            case 8:
                return pixel == 80;
            case 10:
                return pixel == 100 || pixel == 110;
            case 11:
                return pixel == 110 || pixel == 100;
            case 15:
                return pixel == -106;
        }

        return false;
    }

    static boolean isInMultiLcClass(int lcClass, int[] pixels) {
        if (lcClass == 3) {
            int mosaicCroplandCount = 0;
            int naturalVegetationCount = 0;
            for (int pixel : pixels) {
                if (pixel == 30) {
                    mosaicCroplandCount++;
                } else if (pixel == 40) {
                    naturalVegetationCount++;
                }
            }
            return mosaicCroplandCount > pixels.length / 2 && naturalVegetationCount < pixels.length / 2;
        }
        if (lcClass == 4) {
            int naturalVegetationCount = 0;
            int mosaicCroplandCount = 0;
            for (int pixel : pixels) {
                if (pixel == 40) {
                    naturalVegetationCount++;
                } else if (pixel == 30) {
                    mosaicCroplandCount++;
                }
            }
            return naturalVegetationCount > pixels.length / 2 && mosaicCroplandCount < pixels.length / 2;
        }
        if (lcClass == 5) {
            int treeCoverCount = 0;
            for (int pixel : pixels) {
                if (pixel == 50) {
                    treeCoverCount++;
                }
            }
            return treeCoverCount > pixels.length / 100 * 15;
        }
        if (lcClass == 6) {
            int treeCoverCount = 0;
            for (int pixel : pixels) {
                if (pixel == 60) {
                    treeCoverCount++;
                }
            }
            return treeCoverCount > pixels.length / 100 * 15;
        }
        if (lcClass == 7) {
            int treeCoverCount = 0;
            for (int pixel : pixels) {
                if (pixel == 70) {
                    treeCoverCount++;
                }
            }
            return treeCoverCount > pixels.length / 100 * 15;
        }
        if (lcClass == 8) {
            int treeCoverCount = 0;
            for (int pixel : pixels) {
                if (pixel == 80) {
                    treeCoverCount++;
                }
            }
            return treeCoverCount > pixels.length / 100 * 15;
        }
        if (lcClass == 10) {
            int mosaicTreeCount = 0;
            int herbCoverCount = 0;
            for (int pixel : pixels) {
                if (pixel == 100) {
                    mosaicTreeCount++;
                } else if (pixel == 110) {
                    herbCoverCount++;
                }
            }
            return mosaicTreeCount > pixels.length / 2 && herbCoverCount < pixels.length / 2;
        }
        if (lcClass == 11) {
            int herbCoverCount = 0;
            int mosaicTreeCount = 0;
            for (int pixel : pixels) {
                if (pixel == 110) {
                    herbCoverCount++;
                } else if (pixel == 100) {
                    mosaicTreeCount++;
                }
            }
            return herbCoverCount > pixels.length / 2 && mosaicTreeCount < pixels.length / 2;
        }
        if (lcClass == 15) {
            int sparseVegCount = 0;
            for (int pixel : pixels) {
                if (pixel == -106) {
                    sparseVegCount++;
                }
            }
            return sparseVegCount < pixels.length / 100 * 15;
        }
        throw new IllegalArgumentException(String.format("Illegal multi target class: %d", lcClass));
    }

}
