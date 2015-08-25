package com.bc.calvalus.wpsrest.responses;

import static com.bc.calvalus.processing.ProcessorDescriptor.ParameterDescriptor;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusProcessorExtractor;
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
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hans on 13/08/2015.
 */
public class DescribeProcessResponse {

    public ProcessDescriptions getDescribeProcessResponse(Processor processor, CalvalusProcessorExtractor extractor) throws ProductionException {
        ProcessDescriptions processDescriptions = new ProcessDescriptions();

        ProcessDescriptionType processDescription = new ProcessDescriptionType();

        CodeType identifier = getIdentifier(processor.getIdentifier());
        processDescription.setIdentifier(identifier);

        LanguageStringType title = getTitle(processor.getIdentifier());
        processDescription.setTitle(title);

        LanguageStringType abstractText = getAbstractText(processor.getAbstractText());
        processDescription.setAbstract(abstractText);

        DataInputs dataInputs = getDataInputs(processor, extractor);
        processDescription.setDataInputs(dataInputs);

        ProcessOutputs dataOutputs = new ProcessOutputs();
        OutputDescriptionType output = new OutputDescriptionType();

        SupportedComplexDataType supportedComplexDataType = new SupportedComplexDataType();
        ComplexDataCombinationType complexDataCombinationType = new ComplexDataCombinationType();
        ComplexDataDescriptionType complexDataDescriptionType = new ComplexDataDescriptionType();
        complexDataDescriptionType.setSchema("http://schemaOutput.xsd");
        complexDataDescriptionType.setMimeType("binary");
        complexDataCombinationType.setFormat(complexDataDescriptionType);
        supportedComplexDataType.setDefault(complexDataCombinationType);
        output.setComplexOutput(supportedComplexDataType);

        CodeType outputId = getIdentifier("productionResults");
        output.setIdentifier(outputId);

        LanguageStringType abstractOutput = getAbstractText("URL to the production result(s)");
        output.setAbstract(abstractOutput);

        dataOutputs.getOutput().add(output);
        processDescription.setProcessOutputs(dataOutputs);

        processDescriptions.getProcessDescription().add(processDescription);

        return processDescriptions;
    }

    private DataInputs getDataInputs(Processor processor, CalvalusProcessorExtractor extractor) throws ProductionException {
        DataInputs dataInputs = new DataInputs();
        ParameterDescriptor[] parameterDescriptors = processor.getParameterDescriptors();

        InputDescriptionType productionNameInput = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("productionName")
                    .withAbstract("Production name")
                    .withDataType("string")
                    .build();
        dataInputs.getInput().add(productionNameInput);

        InputDescriptionType calvalusBundleVersion = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("calvalus.calvalus.bundle")
                    .withAbstract("Calvalus bundle version")
                    .withDefaultValue(processor.getDefaultCalvalusBundle())
                    .withDataType("string")
                    .build();
        dataInputs.getInput().add(calvalusBundleVersion);

        InputDescriptionType beamBundleVersion = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("calvalus.beam.bundle")
                    .withAbstract("Beam bundle version")
                    .withDefaultValue(processor.getDefaultBeamBundle())
                    .withDataType("string")
                    .build();
        dataInputs.getInput().add(beamBundleVersion);

        if (parameterDescriptors != null) {
            for (ParameterDescriptor parameterDescriptor : parameterDescriptors) {
                InputDescriptionType input = InputDescriptionTypeBuilder
                            .create()
                            .withIdentifier(parameterDescriptor.getName())
                            .withAbstract(parameterDescriptor.getDescription())
                            .withDefaultValue(parameterDescriptor.getDefaultValue())
                            .withDataType(parameterDescriptor.getType())
                            .build();

                dataInputs.getInput().add(input);
            }
        } else if (!StringUtils.isBlank(processor.getDefaultParameters())) {
            InputDescriptionType input = InputDescriptionTypeBuilder
                        .create()
                        .withIdentifier("processorParameters")
                        .withAbstract("Processor parameters")
                        .withDefaultValue(processor.getDefaultParameters())
                        .withDataType("string")
                        .build();

            dataInputs.getInput().add(input);
        }

        List<String> allowedInputPaths = new ArrayList<>();
        for (ProductSet productSet : extractor.getProductSets()) {
            allowedInputPaths.add("/calvalus/" + productSet.getPath());
        }
        InputDescriptionType inputPath = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("inputPath")
                    .withAbstract("Input path")
                    .withDataType("string")
                    .withAllowedValues(allowedInputPaths)
                    .build();

        dataInputs.getInput().add(inputPath);

        InputDescriptionType minDate = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("minDate")
                    .withAbstract("Date from")
                    .withDataType("string")
                    .build();

        dataInputs.getInput().add(minDate);

        InputDescriptionType maxDate = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("maxDate")
                    .withAbstract("Date to")
                    .withDataType("string")
                    .build();

        dataInputs.getInput().add(maxDate);

        InputDescriptionType periodLength = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("periodLength")
                    .withAbstract("Period length")
                    .withDataType("string")
                    .build();

        dataInputs.getInput().add(periodLength);

        InputDescriptionType regionWkt = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("regionWkt")
                    .withAbstract("Region WKT")
                    .withDataType("string")
                    .build();

        dataInputs.getInput().add(regionWkt);

        InputDescriptionType l3ParametersComplexType = getL3ParametersComplexTypeWithSchema("http://schema.xsd");
        dataInputs.getInput().add(l3ParametersComplexType);

        List<String> allowedOutputFormat = getAllowedOutputFormats();
        InputDescriptionType calvalusOutputFormat = InputDescriptionTypeBuilder
                    .create()
                    .withIdentifier("calvalus.output.format")
                    .withAbstract("Calvalus output format")
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
