package com.bc.calvalus.wpsrest.responses;

import static com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;
import static com.bc.calvalus.wpsrest.WpsConstants.WPS_L3_PARAMETERS_SCHEMA_LOCATION;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.CalvalusProcessor;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.CodeType;
import com.bc.calvalus.wpsrest.jaxb.ComplexDataCombinationType;
import com.bc.calvalus.wpsrest.jaxb.ComplexDataCombinationsType;
import com.bc.calvalus.wpsrest.jaxb.ComplexDataDescriptionType;
import com.bc.calvalus.wpsrest.jaxb.InputDescriptionType;
import com.bc.calvalus.wpsrest.jaxb.LanguageStringType;
import com.bc.calvalus.wpsrest.jaxb.OutputDescriptionType;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptionType;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptionType.DataInputs;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptionType.ProcessOutputs;
import com.bc.calvalus.wpsrest.jaxb.ProcessDescriptions;
import com.bc.calvalus.wpsrest.jaxb.SupportedComplexDataInputType;
import com.bc.calvalus.wpsrest.jaxb.SupportedComplexDataType;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusDescribeProcessResponseConverter extends AbstractDescribeProcessResponseConverter {

    public CalvalusDescribeProcessResponseConverter(WpsMetadata wpsMetadata) {
        super(wpsMetadata);
    }

    @Override
    public ProcessDescriptions createBasicProcessDescriptions() {
        ProcessDescriptions processDescriptions = new ProcessDescriptions();
        processDescriptions.setService("WPS");
        processDescriptions.setVersion("1.0.0");
        processDescriptions.setLang("en");
        return processDescriptions;
    }

    @Override
    public ProcessDescriptionType getSingleProcessDescription(IWpsProcess process, WpsMetadata wpsMetadata) {
        ProductSet[] productSets = getProductSets(wpsMetadata);
        CalvalusProcessor calvalusProcessor = (CalvalusProcessor) process;
        ProcessDescriptionType processDescription = new ProcessDescriptionType();

        processDescription.setStoreSupported(true);
        processDescription.setStatusSupported(true);
        processDescription.setProcessVersion(calvalusProcessor.getVersion());

        CodeType identifier = getIdentifier(calvalusProcessor.getIdentifier());
        processDescription.setIdentifier(identifier);

        LanguageStringType title = getTitle(calvalusProcessor.getIdentifier());
        processDescription.setTitle(title);

        LanguageStringType abstractText = getAbstractText(calvalusProcessor.getAbstractText());
        processDescription.setAbstract(abstractText);

        DataInputs dataInputs = getDataInputs(calvalusProcessor, productSets);
        processDescription.setDataInputs(dataInputs);

        ProcessOutputs dataOutputs = new ProcessOutputs();
        OutputDescriptionType output = new OutputDescriptionType();

        SupportedComplexDataType supportedComplexDataType = new SupportedComplexDataType();
        ComplexDataCombinationType complexDataCombinationType = new ComplexDataCombinationType();
        ComplexDataDescriptionType complexDataDescriptionType = new ComplexDataDescriptionType();
        complexDataDescriptionType.setMimeType("binary");
        complexDataCombinationType.setFormat(complexDataDescriptionType);
        supportedComplexDataType.setDefault(complexDataCombinationType);

        ComplexDataCombinationsType complexDataCombinationsType = new ComplexDataCombinationsType();
        complexDataCombinationsType.getFormat().add(complexDataDescriptionType);
        supportedComplexDataType.setSupported(complexDataCombinationsType);

        output.setComplexOutput(supportedComplexDataType);

        CodeType outputId = getIdentifier("productionResults");
        output.setIdentifier(outputId);

        LanguageStringType abstractOutput = getAbstractText("URL to the production result(s)");
        output.setTitle(abstractOutput);

        dataOutputs.getOutput().add(output);
        processDescription.setProcessOutputs(dataOutputs);
        return processDescription;
    }

    private ProductSet[] getProductSets(WpsMetadata wpsMetadata) {
        try {
            CalvalusFacade calvalusFacade = new CalvalusFacade(wpsMetadata.getServletRequestWrapper());
            return calvalusFacade.getProductSets();
        } catch (IOException | ProductionException exception) {
            throw new WpsException("Unable to instanciate CalvalusFacade.", exception);
        }
    }

    private DataInputs getDataInputs(CalvalusProcessor calvalusProcessor, ProductSet[] productSets) {
        DataInputs dataInputs = new DataInputs();
        ParameterDescriptor[] parameterDescriptors = calvalusProcessor.getParameterDescriptors();

        InputDescriptionType productionNameInput = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("productionName")
                    .withTitle("Production name")
                    .withDataType("string")
                    .build();
        dataInputs.getInput().add(productionNameInput);

        if (parameterDescriptors != null) {
            for (ParameterDescriptor parameterDescriptor : parameterDescriptors) {
                InputDescriptionType input = InputDescriptionTypeBuilder
                            .create()
                            .withIdentifier(parameterDescriptor.getName())
                            .withTitle(parameterDescriptor.getDescription())
                            .withDefaultValue(parameterDescriptor.getDefaultValue())
                            .withDataType(parameterDescriptor.getType())
                            .build();

                dataInputs.getInput().add(input);
            }
        } else if (!StringUtils.isBlank(calvalusProcessor.getDefaultParameters())) {
            InputDescriptionType input = InputDescriptionTypeBuilder
                        .create()
                        .withIdentifier("processorParameters")
                        .withTitle("Processor parameters")
                        .withDefaultValue(calvalusProcessor.getDefaultParameters())
                        .withDataType("string")
                        .build();

            dataInputs.getInput().add(input);
        }

        List<String> allowedInputDataSets = new ArrayList<>();
        for (ProductSet productSet : productSets) {
            if (ArrayUtils.contains(calvalusProcessor.getInputProductTypes(), productSet.getProductType())) {
                allowedInputDataSets.add(productSet.getName());
            }
        }
        InputDescriptionType inputDataSetName = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("inputDataSetName")
                    .withTitle("Input data set name")
                    .withDataType("string")
                    .withAllowedValues(allowedInputDataSets)
                    .build();

        dataInputs.getInput().add(inputDataSetName);

        InputDescriptionType minDate = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("minDate")
                    .withTitle("Date from")
                    .withDataType("string")
                    .build();

        dataInputs.getInput().add(minDate);

        InputDescriptionType maxDate = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("maxDate")
                    .withTitle("Date to")
                    .withDataType("string")
                    .build();

        dataInputs.getInput().add(maxDate);

        InputDescriptionType periodLength = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("periodLength")
                    .withTitle("Period length")
                    .withDataType("string")
                    .build();

        dataInputs.getInput().add(periodLength);

        InputDescriptionType regionWkt = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("regionWkt")
                    .withTitle("Region WKT")
                    .withDataType("string")
                    .build();

        dataInputs.getInput().add(regionWkt);

        InputDescriptionType l3ParametersComplexType = getL3ParametersComplexTypeWithSchema(WPS_L3_PARAMETERS_SCHEMA_LOCATION);
        dataInputs.getInput().add(l3ParametersComplexType);

        List<String> allowedOutputFormat = getAllowedOutputFormats();
        InputDescriptionType calvalusOutputFormat = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("calvalus.output.format")
                    .withTitle("Calvalus output format")
                    .withDataType("string")
                    .withAllowedValues(allowedOutputFormat)
                    .build();

        dataInputs.getInput().add(calvalusOutputFormat);

        return dataInputs;
    }

    private InputDescriptionType getL3ParametersComplexTypeWithSchema(String schemaUrl) {
        InputDescriptionType l3ParametersComplexType = new InputDescriptionType();

        CodeType l3ParametersId = getIdentifier("calvalus.l3.parameters");
        l3ParametersComplexType.setIdentifier(l3ParametersId);

        SupportedComplexDataInputType l3Parameters = new SupportedComplexDataInputType();
        ComplexDataCombinationType complexDataCombinationType = new ComplexDataCombinationType();
        ComplexDataDescriptionType complexDataDescriptionType = new ComplexDataDescriptionType();
        complexDataDescriptionType.setSchema(schemaUrl);
        complexDataCombinationType.setFormat(complexDataDescriptionType);
        l3Parameters.setDefault(complexDataCombinationType);

        ComplexDataCombinationsType complexDataCombinationsType = new ComplexDataCombinationsType();
        complexDataCombinationsType.getFormat().add(complexDataDescriptionType);
        l3Parameters.setSupported(complexDataCombinationsType);
        l3ParametersComplexType.setComplexData(l3Parameters);
        return l3ParametersComplexType;
    }

    private List<String> getAllowedOutputFormats() {
        List<String> allowedOutputFormat = new ArrayList<>();
        allowedOutputFormat.add("NetCDF");
        allowedOutputFormat.add("BEAM-DIMAP");
        allowedOutputFormat.add("GeoTIFF");
        return allowedOutputFormat;
    }

    private LanguageStringType getAbstractText(String abstractTextData) {
        LanguageStringType abstractText = new LanguageStringType();
        abstractText.setValue(abstractTextData);
        return abstractText;
    }

    private LanguageStringType getTitle(String titleText) {
        LanguageStringType title = new LanguageStringType();
        title.setValue(titleText);
        return title;
    }

    private CodeType getIdentifier(String identifierText) {
        CodeType identifier = new CodeType();
        identifier.setValue(identifierText);
        return identifier;
    }

}
