package com.bc.calvalus.portal.server;

import org.apache.hadoop.mapreduce.Job;

/**
 * A Hadoop workflow.
 *
 * @author Norman
 */
class HadoopProduction {

    private final String id;
    private final String name;
    private final Job job;

    public HadoopProduction(String id, String name, Job job) {
        //To change body of created methods use File | Settings | File Templates.
        this.id = id;
        this.name = name;
        this.job = job;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Job getJob() {
        return job;
    }
}
