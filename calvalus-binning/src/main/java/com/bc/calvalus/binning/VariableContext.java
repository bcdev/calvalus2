package com.bc.calvalus.binning;

/**
 * A context providing the variables used for the binning.
 *
 * @author Norman Fomferra
 */
public interface VariableContext {
    /**
     * @return The number of variables.
     */
    int getVariableCount();

    /**
     * The index of the variable with the given name.
     *
     * @param name The variable name.
     * @return The index, or -1 if the name is unknown.
     */
    int getVariableIndex(String name);

    /**
     * The name of the variable at the given index.
     *
     * @param index The index.
     * @return The name.
     */
    String getVariableName(int index);

    /**
     * The band-maths expression of a variable at the given index.
     * If non-{@code null}, the expression is used to compute the variable samples.
     * If {@code null}, the variable is expected to be present in the sample sources.
     *
     * @param index The index.
     * @return The expression. May be {@code null}.
     */
    String getVariableExpression(int index);

    /**
     * A Boolean band-maths expression identifying valid source samples.
     *
     * @return The valid-mask expression. May be {@code null}.
     */
    String getValidMaskExpression();
}
