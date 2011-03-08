package com.bc.calvalus.processing;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public interface JobIdFormat<T> {
    JobIdFormat TEXT = new JobIdFormat() {
        @Override
        public String format(Object jobId) {
            return (String) jobId;
        }

        @Override
        public Object parse(String text) {
            return text;
        }
    };

    String format(T jobId);

    T parse(String text);
}
