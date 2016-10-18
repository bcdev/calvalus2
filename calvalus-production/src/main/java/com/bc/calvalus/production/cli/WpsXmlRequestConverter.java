package com.bc.calvalus.production.cli;

import com.bc.calvalus.production.ProductionRequest;

import java.util.Map;

/**
 * A class to convert {@link ProductionRequest} to a WPS XML
 *
 * @author hans
 */
class WpsXmlRequestConverter {

    private final ProductionRequest productionRequest;

    WpsXmlRequestConverter(ProductionRequest productionRequest) {
        this.productionRequest = productionRequest;
    }

    String toXml() {
        return getXmlHeader() +
               getIdentifier(productionRequest.getProductionType()) +
               getDataInputs(productionRequest.getParameters()) +
               "</wps:Execute>";
    }

    private String getXmlHeader() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\" ?>\n" +
               "\n" +
               "<wps:Execute service=\"WPS\"\n" +
               "             version=\"1.0.0\"\n" +
               "             xmlns:wps=\"http://www.opengis.net/wps/1.0.0\"\n" +
               "             xmlns:ows=\"http://www.opengis.net/ows/1.1\"\n" +
               "             xmlns:xlink=\"http://www.w3.org/1999/xlink\"\n" +
               "             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
               "             xsi:schemaLocation=\"http://www.opengis.net/wps/1.0.0 ogc/wps/1.0.0/wpsExecute_request.xsd\">\n";
    }

    private String getIdentifier(String productionType) {
        return "  <ows:Identifier>" + productionType + "</ows:Identifier>\n";
    }

    private String getDataInputs(Map<String, String> parameters) {
        if (parameters.size() == 0) {
            return "  <wps:DataInputs/>\n";
        }
        StringBuilder dataInputsBuilder = new StringBuilder();
        dataInputsBuilder.append("  <wps:DataInputs>\n");
        for (String parameterKey : parameters.keySet()) {
            dataInputsBuilder.append(getLiteralDataInput(parameterKey, parameters.get(parameterKey)));
        }
        dataInputsBuilder.append("  </wps:DataInputs>\n");
        return dataInputsBuilder.toString();
    }

    private String getLiteralDataInput(String key, String value) {
        return "    <wps:Input>\n" +
               "      <ows:Identifier>" + key + "</ows:Identifier>\n" +
               "      <ows:Title/>\n" +
               "      <wps:Data>\n" +
               "        <wps:LiteralData>" + value + "</wps:LiteralData>\n" +
               "      </wps:Data>\n" +
               "    </wps:Input>\n";
    }
}
