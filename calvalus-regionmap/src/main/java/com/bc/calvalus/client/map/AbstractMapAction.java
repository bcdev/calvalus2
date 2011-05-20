package com.bc.calvalus.client.map;

import com.google.gwt.user.client.ui.Image;

public abstract class AbstractMapAction implements MapAction {
    private Image image;
    private String label;
    private String description;

    public AbstractMapAction(Image image, String description) {
        this.image = image;
        this.description = description;
    }

    public AbstractMapAction(String label, String description) {
        this.label = label;
        this.description = description;
    }

    protected AbstractMapAction(String label, Image image, String description) {
        this.label = label;
        this.image = image;
        this.description = description;
    }

    @Override
    public Image getImage() {
        return image;
    }

    public void setImage(Image image) {
        this.image = image;
    }

    @Override
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
