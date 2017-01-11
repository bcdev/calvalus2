package com.bc.calvalus.portal.client.map;

import com.google.gwt.user.client.ui.Image;

/**
 * An action performed on a region map.
 *
 * @author Norman
 */
public interface MapAction {
    MapAction SEPARATOR = new Separator();

    Image getImage();

    String getDescription();

    String getLabel();

    void run(RegionMap regionMap);

    class Separator implements MapAction {
        @Override
        public Image getImage() {
            return null;
        }

        @Override
        public String getDescription() {
            return null;
        }

        @Override
        public String getLabel() {
            return null;
        }

        @Override
        public void run(RegionMap regionMap) {
        }
    }
}
