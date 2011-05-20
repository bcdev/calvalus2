package com.bc.calvalus.client.map;

import com.google.gwt.user.client.ui.Image;

/**
 * An interactor is a region map action that lets a user first interact with a map
 * before the actual action is performed.
 *
 * @author Norman
 */
public abstract class MapInteraction implements MapAction {
    private final MapAction action;

    /**
     * @param action The actual action to be performed, once the interaction is complete.
     */
    protected MapInteraction(MapAction action) {
        this.action = action;
    }

    @Override
    public Image getImage() {
        return action.getImage();
    }

    @Override
    public String getDescription() {
        return action.getDescription();
    }

    @Override
    public String getLabel() {
        return action.getLabel();
    }

    /**
     * Called by sub-classes, once interaction is complete.
     *
     * @param regionMap The region map.
     */
    @Override
    public void run(RegionMap regionMap) {
        action.run(regionMap);
    }

    /**
     * Attaches this interaction to the given region map.
     *
     * @param regionMap The region map.
     */
    public abstract void attachTo(RegionMap regionMap);

    /**
     * Detaches this interaction from the given region map.
     *
     * @param regionMap The region map.
     */
    public abstract void detachFrom(RegionMap regionMap);
}
