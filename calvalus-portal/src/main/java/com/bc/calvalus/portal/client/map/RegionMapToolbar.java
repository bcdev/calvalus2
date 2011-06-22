package com.bc.calvalus.portal.client.map;

import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.HorizontalPanel;
import com.google.gwt.user.client.ui.IsWidget;
import com.google.gwt.user.client.ui.PushButton;
import com.google.gwt.user.client.ui.ToggleButton;
import com.google.gwt.user.client.ui.Widget;

import java.util.HashMap;
import java.util.Map;

/**
 * A map control that lets a user select the current map interactor.
 */
public class RegionMapToolbar implements IsWidget {

    private final RegionMap regionMap;

    private Map<ToggleButton, MapInteraction> interactions;
    private Map<MapInteraction, ToggleButton> interactionButtons;
    private MapInteraction currentInteraction;
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

    private void initWidget() {

        interactions = new HashMap<ToggleButton, MapInteraction>();
        interactionButtons = new HashMap<MapInteraction, ToggleButton>();

        ClickHandler interactionClickHandler = new ClickHandler() {
            @Override
            public void onClick(ClickEvent clickEvent) {
                ToggleButton selectedToggleButton = (ToggleButton) clickEvent.getSource();
                MapInteraction interaction = interactions.get(selectedToggleButton);
                setCurrentInteraction(interaction);

                for (ToggleButton interactorButton : interactions.keySet()) {
                    if (selectedToggleButton != interactorButton && interactorButton.isDown()) {
                        interactorButton.setDown(false);
                    }
                }
            }
        };

        HorizontalPanel buttonPanel = new HorizontalPanel();
        buttonPanel.setSpacing(2);

        for (final MapAction action : regionMap.getRegionModel().getActions()) {
            if (action instanceof MapAction.Separator) {
                buttonPanel.add(new HTML("&nbsp;"));
            } else if (action instanceof MapInteraction) {
                MapInteraction interaction = (MapInteraction) action;
                // todo - use interaction.getIcon() images here (nf)
                ToggleButton toggleButton = new ToggleButton(interaction.getLabel(), interactionClickHandler);
                toggleButton.setTitle(interaction.getDescription());
                interactions.put(toggleButton, interaction);
                interactionButtons.put(interaction, toggleButton);
                if (currentInteraction == null) {
                    currentInteraction = interaction;
                    currentInteraction.attachTo(regionMap);
                    toggleButton.setDown(true);
                }
                buttonPanel.add(toggleButton);
            } else {
                // todo - use interaction.getIcon() images here (nf)
                PushButton pushButton = new PushButton(action.getLabel(), new ClickHandler() {
                    @Override
                    public void onClick(ClickEvent clickEvent) {
                        action.run(regionMap);
                    }
                });
                pushButton.setTitle(action.getDescription());
                buttonPanel.add(pushButton);
            }
        }
        this.widget = buttonPanel;
    }

    public MapInteraction getCurrentInteraction() {
        return currentInteraction;
    }

    public void setCurrentInteraction(MapInteraction mapInteraction) {
        if (currentInteraction != null) {
            currentInteraction.detachFrom(regionMap);
        }
        currentInteraction = mapInteraction;
        if (currentInteraction != null) {
            currentInteraction.attachTo(regionMap);
        }
    }
}
