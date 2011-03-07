package com.bc.calvalus.production;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public interface JobIdFormat<T> {
    String format(T jobId);
    T parse(String text);
}
