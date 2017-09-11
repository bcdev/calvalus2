/*
 * Copyright (C) 2015 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.portal.client;

import com.google.gwt.resources.client.CssResource;

/**
 * @author marcoz
 */
interface CalvalusStyle extends CssResource {

    String explanatoryValue();

    String explanatoryLabel();

    String centeredHorizontalPanel();

    String anchor();

    String variableExpression();

    String variableName();

    String panelTitle();

    String panelTitleText();

    String inputFileSetPanel();

    String checkBox();

    String temporalFilterPanel();

    String filterPanel();

    String spatialFilterPanel();

    String radioButton();

    String dateBox();

    String textBox();

    String l2Panel();

    String l2ParametersPanel();

    String processorParametersPanels();

    String l2ProcessorPanel();

    String l2ProcessorPrefMulti();

    String l2ProcessorDescription();

    String l2Help();

    String l2ProcessorPrefContainer();

    String l2ParametersButtonGroup();

    String l2ParametersFileUpload();

    String outputParametersPanel();

    String processingFormatPanel();

    String productionNameTextBox();

    String productionNameText();

    String outputProductionName();

    String flexRow();

    String noteLabel();
}
