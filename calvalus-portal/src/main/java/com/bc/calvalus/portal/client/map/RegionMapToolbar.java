package com.bc.calvalus.portal.client.map;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.*;

import java.util.HashMap;
import java.util.Map;

/**
 * A map control that lets a user select the current map interactor.
 */
public class RegionMapToolbar implements IsWidget {

    private final RegionMap regionMap;

    private Map<ToggleButton, MapInteraction> interactions;
    private Map<MapInteraction, ToggleButton> interactionButtons;
    private Widget widget;

    public RegionMapToolbar(RegionMap regionMap) {
        this.regionMap = regionMap;
    }

    @Override
    public Widget asWidget() {
        if (widget == null) {
            initWidget();
        }
        return widget;
    }

    public void select(MapInteraction interaction) {
        ToggleButton button = interactionButtons.get(interaction);
        if (button != null && !button.isDown()) {
            button.setDown(true);
        }
    }

    public void deselect(MapInteraction interaction) {
        ToggleButton button = interactionButtons.get(interaction);
        if (button != null && button.isDown()) {
            button.setDown(false);
        }
    }

    private void initWidget() {

        interactions = new HashMap<ToggleButton, MapInteraction>();
        interactionButtons = new HashMap<MapInteraction, ToggleButton>();

        ClickHandler interactionClickHandler = new ClickHandler() {
            @Override
            public void onClick(ClickEvent clickEvent) {
                ToggleButton selectedToggleButton = (ToggleButton) clickEvent.getSource();
                MapInteraction interaction = interactions.get(selectedToggleButton);
                regionMap.setCurrentInteraction(interaction);

                for (ToggleButton interactorButton : interactions.keySet()) {
                    if (selectedToggleButton != interactorButton && interactorButton.isDown()) {
                        interactorButton.setDown(false);
                    }
                }
            }
        };

        final VerticalPanel buttonPanel = new VerticalPanel();
        buttonPanel.setSpacing(2);
        HorizontalPanel buttonPanel1 = new HorizontalPanel();
        HorizontalPanel buttonPanel2 = new HorizontalPanel();
        buttonPanel.add(buttonPanel1);
        buttonPanel.add(buttonPanel2);
        buttonPanel1.setSpacing(2);
        buttonPanel2.setSpacing(2);

        HorizontalPanel currentPanel = buttonPanel1;
        for (final MapAction action : regionMap.getActions()) {
            if (action instanceof MapAction.Separator) {
                //buttonPanel.add(new HTML("&nbsp;"));
                currentPanel = buttonPanel2;
            } else if (action instanceof MapInteraction) {
                MapInteraction interaction = (MapInteraction) action;
                ToggleButton toggleButton;
                if (interaction.getImage() != null) {
                    toggleButton = new ToggleButton(interaction.getImage(), interactionClickHandler);
                } else {
                    toggleButton = new ToggleButton(interaction.getLabel(), interactionClickHandler);
                }
                toggleButton.setTitle(interaction.getDescription());
                interactions.put(toggleButton, interaction);
                interactionButtons.put(interaction, toggleButton);
                if (regionMap.getCurrentInteraction() == null) {
                    regionMap.setCurrentInteraction(interaction);
                    toggleButton.setDown(true);
                }
                currentPanel.add(toggleButton);
            } else {
                PushButton pushButton;
                if (action.getImage() != null) {
                    pushButton = new PushButton(action.getImage(), new ClickHandler() {
                        @Override
                        public void onClick(ClickEvent clickEvent) {
                            action.run(regionMap);
                        }
                    });
                } else {
                    pushButton = new PushButton(action.getLabel(), new ClickHandler() {
                        @Override
                        public void onClick(ClickEvent clickEvent) {
                            action.run(regionMap);
                        }
                    });
                }
                pushButton.setTitle(action.getDescription());
                currentPanel.add(pushButton);
            }
        }

        this.widget = buttonPanel;
    }
}
