package org.esa.beam.binning;

/**
 * Configuration of a binning variable.
 *
 * @author Norman Fomferra
 * @see com.bc.calvalus.binning.VariableContext
 */
public class VariableConfig {
    private String name;

    private String expr;

    public VariableConfig() {
    }

    public VariableConfig(String name, String expr) {
        this.name = name;
        this.expr = expr;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VariableConfig that = (VariableConfig) o;

        if (!name.equals(that.name)) return false;
        if (!expr.equals(that.expr)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + expr.hashCode();
        return result;
    }
}
