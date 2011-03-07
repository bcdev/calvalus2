package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.production.JobIdFormat;
import org.apache.hadoop.mapreduce.JobID;

/**
* todo - add api doc
*
* @author Norman Fomferra
*/
class HadoopJobIdFormat implements JobIdFormat<JobID> {
    @Override
    public String format(JobID jobId) {
        return jobId.toString();
    }

    @Override
    public JobID parse(String text) {
        return JobID.forName(text);
    }
}
