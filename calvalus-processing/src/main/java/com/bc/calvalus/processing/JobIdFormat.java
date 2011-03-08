package com.bc.calvalus.processing;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public interface JobIdFormat<T> {
    JobIdFormat<String> TEXT = new JobIdFormat<String>() {
        @Override
        public String format(String jobId) {
            return jobId;
        }

        @Override
        public String parse(String text) {
            return text;
        }
    };

    String format(T jobId);

    T parse(String text);
}
