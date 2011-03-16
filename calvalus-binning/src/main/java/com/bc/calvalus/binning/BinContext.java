package com.bc.calvalus.binning;

/**
 * A context is valid for a single bin.
 */
public interface BinContext {

    /**
     * Gets a named value from a temporary context. The context is valid for a
     * single bin. Values are shared between all aggregators operating on that bin.
     *
     * @param name The value name.
     * @param <T>  The value's type.
     * @return The value, may be {@code null}.
     */
    <T> T get(String name);

    /**
     * Sets a named value in a temporary context. The context is valid for a
     * single bin. Values are shared between all aggregators operating on that bin.
     *
     * @param name The value name.
     * @param value  The value.
     */
    void put(String name, Object value);
}
