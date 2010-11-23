package com.bc.calvalus.plot;

import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.text.ParseException;

%%

%class RunTimesScanner
%implements Scanner
%type Trace    // return type of the scanner ist Trace
%yylexthrow ParseException

%{

    private final List<Trace> traces = new ArrayList<Trace>();
    private final Map<String,Trace> openTraces = new HashMap<String,Trace>();
    private final Valids valids = new Valids();

    public enum Keys { // todo ? sinnvoll ? 
        DATA, JOB, TYPE, HOST
    }

    protected String start;
    protected String stop;

    private String datetime;
    private String jobId;
    private String attemptId;
    private String taskId;
    private String hostname;
    private String numberOfSplits;
    private String taskType;
    private Trace trace;

    static {
        TimeUtils.CCSDS_UTC_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public void init() {
        traces.clear();
        openTraces.clear();
        valids.clear();
        start = null;
        stop = null;
    }

    public List<Trace> getTraces() {
        return traces;
    }

    public Valids getValids() {
        return valids;
    }

     public List<Trace> scan() {
        this.init();
        try {
            while (this.yylex() != null) ;
            return traces;
        } catch (Exception e) {
            e.printStackTrace();   // todo
            return null;    
        }
    }

   static class Valids {
       private final Map<String, Set<String>> attributes = new HashMap<String, Set<String>>();

        public void clear() {
            attributes.clear();
        }

        public void add(String key, String value) {
            Set<String> valids = attributes.get(key);
            if (valids == null) {
                valids = new HashSet<String>();
                attributes.put(key, valids);
            }
            valids.add(value);
        }

        public Set<String> keySet() {
            return attributes.keySet();
        }

        public Set<String> get(String key) {
            return attributes.get(key);
        }

        public String toString() {
            StringBuffer accu = new StringBuffer(1024);
            for (String k : keySet()) {
                if (accu.length() > 0) accu.append('\n');
                accu.append(k);
                accu.append(":(");
                for (String v : get(k)) {
                    if (accu.charAt(accu.length() - 1) != '(') accu.append(',');
                    accu.append(v);
                }
                accu.append(')');
            }
            return accu.toString();
        }
}


%}

datetime=[0-9]{4}-[0-9]{2}-[0-9]{2}" "[0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}
jobid=[0-9]{12}_[0-9]{4}
taskid={jobid}_[mr]_[0-9]{6}
attemptid={jobid}_[mr]_[0-9]{6}_[0-9]

%x DATETIME, NEWJOB, JOBSIZE, LOOKFORSPLITS, NUMBEROFSPLITS, NEWTASK, LOOKFORHOST, HOSTNAME, DATALOCALTASK, RACKLOCALTASK, TASKSTOP, JOBSTOP, NEXT

%%

/*
2010-10-28 10:16:29,738 INFO org.apache.hadoop.mapred.JobTracker: Initializing job_201010281009_0001
2010-10-28 10:16:30,010 INFO org.apache.hadoop.mapred.JobInProgress: Input size for job job_201010281009_0001 = 56716716204. Number of splits = 160
2010-10-28 10:16:46,478 INFO org.apache.hadoop.mapred.JobTracker: Adding task 'attempt_201010281009_0001_m_000004_0' to tip task_201010281009_0001_m_000004, for tracker 'tracker_cvslave02.bc.local:localhost/127.0.0.1:60416'
2010-10-28 10:16:46,482 INFO org.apache.hadoop.mapred.JobInProgress: Choosing data-local task task_201010281009_0001_m_000004
2010-10-28 10:17:16,587 INFO org.apache.hadoop.mapred.JobInProgress: Task 'attempt_201010281009_0001_m_000004_0' has completed task_201010281009_0001_m_000004 successfully.
2010-10-28 10:36:54,027 INFO org.apache.hadoop.mapred.JobInProgress: Job job_201010281009_0003 has completed successfully.
*/

<YYINITIAL>{datetime}	{
			    datetime=yytext();
                            if (start == null /* || datetime.compareTo(start) < 0 */) start = datetime;
                            /* if (stop == null || datetime.compareTo(stop) > 0) */ stop = datetime;
			    yybegin(DATETIME);
			}

<YYINITIAL>\n		{
			}

<YYINITIAL>.		{
			    yybegin(NEXT);
			}

/* job start */

<DATETIME>" INFO org.apache.hadoop.mapred.JobTracker: Initializing job_"	{
			    yybegin(NEWJOB);
			}

<NEWJOB>{jobid}		{
			    jobId = yytext();
                            trace = new Trace(jobId);
                            trace.setStartTime(TimeUtils.parseCcsdsLocalTimeWithoutT(datetime));
                            trace.setProperty(Keys.TYPE.name(), "job");
                            traces.add(trace);
                            openTraces.put(jobId, trace);
                            valids.add("job", jobId);
                            valids.add(Keys.TYPE.name(), "job");
			    yybegin(NEXT);
			}

/* job size */

<DATETIME>" INFO org.apache.hadoop.mapred.JobInProgress: Input size for job job_"	{
			    yybegin(JOBSIZE);
			}

<JOBSIZE>{jobid}	{
			    jobId = yytext();
                            yybegin(LOOKFORSPLITS);
			}

<LOOKFORSPLITS>.*"Number of splits = "	{
			    yybegin(NUMBEROFSPLITS);
			}


<NUMBEROFSPLITS>[0-9]*		{
			    numberOfSplits = yytext();
                            trace = openTraces.get(jobId);
                            if (trace != null) {
                                trace.setProperty("splits", numberOfSplits);
                            }
			    yybegin(NEXT);
                            if (trace != null) return trace;
			}

/* task start */

<DATETIME>" INFO org.apache.hadoop.mapred.JobTracker: Adding task 'attempt_"	{
			    yybegin(NEWTASK);
			}

<NEWTASK>{attemptid}	{
			    attemptId = yytext();
                            taskId = attemptId.substring(0,26);
                            jobId = attemptId.substring(0,17);
                            taskType = taskId.substring(18,19);
			    yybegin(LOOKFORHOST);
			}

<LOOKFORHOST>.*" for tracker 'tracker_"	{
			    yybegin(HOSTNAME);
			}

<HOSTNAME>[a-zA-Z0-9_-]*	{
			    hostname = yytext();
                            trace = new Trace(attemptId);
                            trace.setStartTime(TimeUtils.parseCcsdsLocalTimeWithoutT(datetime));
                            trace.setProperty(Keys.TYPE.name(), taskType);
                            trace.setProperty(Keys.HOST.name(), hostname);
                            trace.setProperty("job", jobId);
                            traces.add(trace);
                            openTraces.put(taskId, trace);
                            valids.add("job", jobId);
                            valids.add(Keys.TYPE.name(), taskType);
                            valids.add(Keys.HOST.name(), hostname);
			    yybegin(NEXT);
			}

/* data local */

<DATETIME>" INFO org.apache.hadoop.mapred.JobInProgress: Choosing data-local task task_"	{
			    yybegin(DATALOCALTASK);
			}

<DATALOCALTASK>{taskid}	{
			    taskId = yytext();
                            trace = openTraces.get(taskId);
                            if (trace != null) {
                                trace.setProperty(Keys.DATA.name(), "local");
                                valids.add(Keys.DATA.name(), "local");
                            }
                            yybegin(NEXT);
                            return trace;
			}

<DATETIME>" INFO org.apache.hadoop.mapred.JobInProgress: Choosing rack-local task task_"	{
			    yybegin(RACKLOCALTASK);
			}

<RACKLOCALTASK>{taskid}	{
			    taskId = yytext();
                            trace = openTraces.get(taskId);
                            if (trace != null) {
                                trace.setProperty(Keys.DATA.name(), "remote");
                                valids.add(Keys.DATA.name(), "remote");
                            }
                            yybegin(NEXT);
                            return trace;
			}

/* task stop */

<DATETIME>" INFO org.apache.hadoop.mapred.JobInProgress: Task 'attempt_"	{
			    yybegin(TASKSTOP);
			}

<TASKSTOP>{attemptid}	{
			    attemptId = yytext();
                            taskId = attemptId.substring(0,26);
                            trace = openTraces.get(taskId);
                            if (trace != null) {
                                trace.setStopTime(TimeUtils.parseCcsdsLocalTimeWithoutT(datetime));
                                openTraces.remove(taskId);
                            }
                            yybegin(NEXT);
                            if (trace != null) return trace;
			}

/* job stop */

<DATETIME>" INFO org.apache.hadoop.mapred.JobInProgress: Job job_"	{
			    yybegin(JOBSTOP);
			}

<JOBSTOP>{jobid}	{
			    jobId = yytext();
                            trace = openTraces.get(jobId);
                            if (trace != null) {
                                trace.setStopTime(TimeUtils.parseCcsdsLocalTimeWithoutT(datetime));
                                openTraces.remove(jobId);
                            }
                            yybegin(NEXT);
                            if (trace != null) return trace;
			}

/* other stuff */

<DATETIME>.		{
			    yybegin(NEXT);
			}

<NEXT>.*\n		{
			    yybegin(YYINITIAL);
			}

<<EOF>>			{
			    return null;
			}
