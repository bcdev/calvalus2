package com.bc.calvalus.reporting;

import java.text.DateFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author hans
 */
public class ReportGenerator {

    public void generatePdfSingleJob(UsageStatistic usageStatistic) {
        String reports = getSingleJobReportContents(usageStatistic);
    }

    public String generateTextSingleJob(UsageStatistic usageStatistic) {
        return getSingleJobReportContents(usageStatistic);
    }

    public void generatePdfMonthly(List<UsageStatistic> usageStatistics) {

    }

    public String generateTextMonthly(List<UsageStatistic> usageStatistics) {
        return getMultiJobReportContents(usageStatistics);
    }

    private String getSingleJobReportContents(UsageStatistic usageStatistic) {
        String startTime = getFormattedTime(usageStatistic.getStartTime());
        String finishTime = getFormattedTime(usageStatistic.getFinishTime());
        String totalTime = getElapsedTime(usageStatistic.getTotalTime());
        String totalFileWriting = getFormattedNumber((usageStatistic.getFileBytesWritten() + usageStatistic.getHdfsBytesWritten()) / (1024 * 1024));
        String totalFileReading = getFormattedNumber((usageStatistic.getFileBytesRead() + usageStatistic.getHdfsBytesRead()) / (1024 * 1024));
        String totalCpuTime = getElapsedTime(usageStatistic.getCpuMilliseconds());
        String totalMemoryUsed = getFormattedNumber((usageStatistic.getMbMillisMaps() + usageStatistic.getMbMillisReduces()) / (1000));
        String vCoresUsed = getFormattedNumber((usageStatistic.getvCoresMillisMaps() + usageStatistic.getvCoresMillisReduces()) / (1000));
        return "Usage statistic for job '" + usageStatistic.getJobId() + "'\n" +
               "\n" +
               "Project : " + usageStatistic.getQueueName() + "\n" +
               "Start time : " + startTime + "\n" +
               "Finish time : " + finishTime + "\n" +
               "Total time : " + totalTime + "\n" +
               "Status :  " + usageStatistic.getStatus() + "\n" +
               "Total file writing (MB) : " + totalFileWriting + "\n" +
               "Total file reading (MB) : " + totalFileReading + "\n" +
               "Total CPU time spent : " + totalCpuTime + "\n" +
               "Total Memory used (MB s) :  " + totalMemoryUsed + "\n" +
               "Total vCores used (vCore s) :  " + vCoresUsed + "\n";
    }

    private String getMultiJobReportContents(List<UsageStatistic> usageStatistics) {
        long totalFileWriting = 0;
        long totalFileReading = 0;
        long totalCpuTime = 0;
        long totalMemoryUsed = 0;
        long totalVCoresUsed = 0;
        for (UsageStatistic usageStatistic : usageStatistics) {
            totalFileWriting += (usageStatistic.getFileBytesWritten() + usageStatistic.getHdfsBytesWritten()) / (1024 * 1024);
            totalFileReading += (usageStatistic.getFileBytesRead() + usageStatistic.getHdfsBytesRead()) / (1024 * 1024);
            totalCpuTime += usageStatistic.getCpuMilliseconds();
            totalMemoryUsed += (usageStatistic.getMbMillisMaps() + usageStatistic.getMbMillisReduces()) / (1000);
            totalVCoresUsed += (usageStatistic.getvCoresMillisMaps() + usageStatistic.getvCoresMillisReduces()) / (1000);
        }
        int jobNumbers = usageStatistics.size();
        return "Usage statistic for user $USER in $MONTH $YEAR\n" +
               "\n" +
               "Jobs processed : " + jobNumbers + "\n" +
               "Total file writing (MB) : " + getFormattedNumber(totalFileWriting) + "\n" +
               "Total file reading (MB) : " + getFormattedNumber(totalFileReading) + "\n" +
               "Total CPU time spent : " + getElapsedTime(totalCpuTime) + "\n" +
               "Total Memory used (MB s) :  " + getFormattedNumber(totalMemoryUsed) + "\n" +
               "Total vCores used (vCore s) :  " + getFormattedNumber(totalVCoresUsed) + "\n";
    }

    private String getFormattedNumber(long number) {
        return String.format("%,d", number);
    }

    private String getElapsedTime(long totalTime) {
        long hours = TimeUnit.MILLISECONDS.toHours(totalTime);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(totalTime - TimeUnit.HOURS.toMillis(hours));
        long seconds = TimeUnit.MILLISECONDS.toSeconds(totalTime - TimeUnit.HOURS.toMillis(hours) - TimeUnit.MINUTES.toMillis(minutes));
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    private String getFormattedTime(long startTime) {
        DateFormat df = DateFormat.getDateTimeInstance();
        return df.format(startTime);
    }
}
