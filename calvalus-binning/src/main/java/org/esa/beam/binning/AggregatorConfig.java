package org.esa.beam.binning;

import java.util.Arrays;

/**
 * Configuration of a binning aggregator.
 *
 * @author Norman Fomferra
 * @see com.bc.calvalus.binning.Aggregator
 */
public class AggregatorConfig {
    private String type;

    private String varName;

    private String[] varNames;

    private Integer percentage;

    private Double weightCoeff;

    private Float fillValue;

    public AggregatorConfig() {
    }

    public AggregatorConfig(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public String[] getVarNames() {
        return varNames;
    }

    public void setVarNames(String[] varNames) {
        this.varNames = varNames;
    }

    public Integer getPercentage() {
        return percentage;
    }

    public void setPercentage(Integer percentage) {
        this.percentage = percentage;
    }

    public Double getWeightCoeff() {
        return weightCoeff;
    }

    public void setWeightCoeff(Double weightCoeff) {
        this.weightCoeff = weightCoeff;
    }

    public Float getFillValue() {
        return fillValue;
    }

    public void setFillValue(Float fillValue) {
        this.fillValue = fillValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregatorConfig that = (AggregatorConfig) o;

        if (fillValue != null ? !fillValue.equals(that.fillValue) : that.fillValue != null) return false;
        if (percentage != null ? !percentage.equals(that.percentage) : that.percentage != null) return false;
        if (!type.equals(that.type)) return false;
        if (varName != null ? !varName.equals(that.varName) : that.varName != null) return false;
        if (!Arrays.equals(varNames, that.varNames)) return false;
        if (weightCoeff != null ? !weightCoeff.equals(that.weightCoeff) : that.weightCoeff != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (varName != null ? varName.hashCode() : 0);
        result = 31 * result + (varNames != null ? Arrays.hashCode(varNames) : 0);
        result = 31 * result + (percentage != null ? percentage.hashCode() : 0);
        result = 31 * result + (weightCoeff != null ? weightCoeff.hashCode() : 0);
        result = 31 * result + (fillValue != null ? fillValue.hashCode() : 0);
        return result;
    }
}
