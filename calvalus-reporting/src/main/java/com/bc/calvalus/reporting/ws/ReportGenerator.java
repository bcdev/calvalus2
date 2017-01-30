package com.bc.calvalus.reporting.ws;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;

/**
 * @author hans
 */
class ReportGenerator {

    public String generateJsonAllUserJobSummary(Map<String, List<UsageStatistic>> allUserStatistics) {
        List<String> stringList = new ArrayList<>();
        allUserStatistics.forEach((key, usageStatistics) -> {
            String stringListBiConsumer = generateJsonUserSingleJob(usageStatistics);
            stringList.add(stringListBiConsumer);
        });
        Gson gson = new Gson();
        return gson.toJson(stringList);
    }

    String generatePdfSingleJob(UsageStatistic usageStatistic) throws IOException {
        PDDocument document = new PDDocument();
        PDPage page = new PDPage();
        document.addPage(page);
        List<String> reportContents = getSingleJobReportContents(usageStatistic);
        writeToPdfPage(document, page, reportContents);
        File reportPdf = new File(usageStatistic.getJobId() + ".pdf");
        document.save(reportPdf);
        document.close();
        return reportPdf.getAbsolutePath();
    }

    String generateTextSingleJob(UsageStatistic usageStatistic) {
        StringBuilder stringBuilder = new StringBuilder();
        List<String> report = getSingleJobReportContents(usageStatistic);
        for (String line : report) {
            stringBuilder.append(line).append("\n");
        }
        return stringBuilder.toString();
    }

    String generateJsonSingleJob(UsageStatistic usageStatistic) {
        Map<String, String> report = getSingleJobJsonContents(usageStatistic);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(report);
    }

    String generateJsonUserSingleJob(List<UsageStatistic> usageStatistics) {
        Map<String, String> report = getMultiJobJsonContents(usageStatistics);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(report);
    }

    String generatePdfMonthly(List<UsageStatistic> usageStatistics) throws IOException {
        PDDocument document = new PDDocument();
        PDPage summaryPage = new PDPage();
        document.addPage(summaryPage);
        List<String> multiJobReportContents = getMultiJobReportContents(usageStatistics);
        writeToPdfPage(document, summaryPage, multiJobReportContents);
        for (UsageStatistic usageStatistic : usageStatistics) {
            PDPage singleJobPage = new PDPage();
            document.addPage(singleJobPage);
            List<String> singleJobReportContents = getSingleJobReportContents(usageStatistic);
            writeToPdfPage(document, singleJobPage, singleJobReportContents);
        }
        File reportPdf = new File("monthly.pdf");
        document.save(reportPdf);
        document.close();
        return reportPdf.getAbsolutePath();
    }

    String generateTextMonthly(List<UsageStatistic> usageStatistics) {
        StringBuilder stringBuilder = new StringBuilder();
        List<String> report = getMultiJobReportContents(usageStatistics);
        for (String line : report) {
            stringBuilder.append(line).append("\n");
        }
        return stringBuilder.toString();
    }

    private void writeToPdfPage(PDDocument document, PDPage page, List<String> reports) throws IOException {
        try (PDPageContentStream contentStream = new PDPageContentStream(document, page, PDPageContentStream.AppendMode.APPEND, false)) {
            contentStream.beginText();
            contentStream.setFont(PDType1Font.COURIER, 12);
            contentStream.newLineAtOffset(25, 750);
            for (String reportLine : reports) {
                contentStream.showText(reportLine);
                contentStream.newLineAtOffset(0, -15);
            }
            contentStream.endText();
        }
    }

    private Map<String, String> getSingleJobJsonContents(UsageStatistic usageStatistic) {
        Map<String, String> jobReportJson = new HashMap<>();
        String startTime = getFormattedTime(usageStatistic.getStartTime());
        String finishTime = getFormattedTime(usageStatistic.getFinishTime());
        String totalTime = getElapsedTime(usageStatistic.getTotalTime());
        String totalFileWriting = getFormattedNumber((usageStatistic.getFileBytesWritten() + usageStatistic.getHdfsBytesWritten()) / (1024 * 1024));
        String totalFileReading = getFormattedNumber((usageStatistic.getFileBytesRead() + usageStatistic.getHdfsBytesRead()) / (1024 * 1024));
        String totalCpuTime = getElapsedTime(usageStatistic.getCpuMilliseconds());
        String totalMemoryUsed = getFormattedNumber((usageStatistic.getMbMillisTotal()) / (1000));
        String vCoresUsed = getFormattedNumber((usageStatistic.getvCoresMillisTotal()) / (1000));
        jobReportJson.put("jobId", usageStatistic.getJobId());
        jobReportJson.put("project", usageStatistic.getQueue());
        jobReportJson.put("startTime", startTime);
        jobReportJson.put("finishTime", finishTime);
        jobReportJson.put("totalTime", totalTime);
        jobReportJson.put("status", usageStatistic.getState());
        jobReportJson.put("totalFileWritingMb", totalFileWriting);
        jobReportJson.put("totalFileReadingMb", totalFileReading);
        jobReportJson.put("totalCpuTime", totalCpuTime);
        jobReportJson.put("totalMemoryUsedMbs", totalMemoryUsed);
        jobReportJson.put("totalVcoresUsed", vCoresUsed);
        return jobReportJson;
    }

    private List<String> getSingleJobReportContents(UsageStatistic usageStatistic) {
        List<String> jobReport = new ArrayList<>();
        String startTime = getFormattedTime(usageStatistic.getStartTime());
        String finishTime = getFormattedTime(usageStatistic.getFinishTime());
        String totalTime = getElapsedTime(usageStatistic.getTotalTime());
        String totalFileWriting = getFormattedNumber((usageStatistic.getFileBytesWritten() + usageStatistic.getHdfsBytesWritten()) / (1024 * 1024));
        String totalFileReading = getFormattedNumber((usageStatistic.getFileBytesRead() + usageStatistic.getHdfsBytesRead()) / (1024 * 1024));
        String totalCpuTime = getElapsedTime(usageStatistic.getCpuMilliseconds());
        String totalMemoryUsed = getFormattedNumber((usageStatistic.getMbMillisTotal()) / (1000));
        String vCoresUsed = getFormattedNumber((usageStatistic.getvCoresMillisTotal()) / (1000));
        jobReport.add("Usage statistic for job '" + usageStatistic.getJobId() + "'");
        jobReport.add("");
        jobReport.add("Project : " + usageStatistic.getQueue());
        jobReport.add("Start time : " + startTime);
        jobReport.add("Finish time : " + finishTime);
        jobReport.add("Total time : " + totalTime);
        jobReport.add("Status :  " + usageStatistic.getState());
        jobReport.add("Total file writing (MB) : " + totalFileWriting);
        jobReport.add("Total file reading (MB) : " + totalFileReading);
        jobReport.add("Total CPU time spent : " + totalCpuTime);
        jobReport.add("Total Memory used (MB s) :  " + totalMemoryUsed);
        jobReport.add("Total vCores used (vCore s) :  " + vCoresUsed);
        return jobReport;
    }

    private Map<String, String> getMultiJobJsonContents(List<UsageStatistic> usageStatistics) {
        Map<String, String> jobReportJson = new HashMap<>();
        long totalFileWriting = 0;
        long totalFileReading = 0;
        long totalCpuTime = 0;
        long totalMemoryUsed = 0;
        long totalVCoresUsed = 0;
        for (UsageStatistic usageStatistic : usageStatistics) {
            totalFileWriting += (usageStatistic.getFileBytesWritten() + usageStatistic.getHdfsBytesWritten()) / (1024 * 1024);
            totalFileReading += (usageStatistic.getFileBytesRead() + usageStatistic.getHdfsBytesRead()) / (1024 * 1024);
            totalCpuTime += usageStatistic.getCpuMilliseconds();
            totalMemoryUsed += (usageStatistic.getMbMillisTotal()) / (1000);
            totalVCoresUsed += (usageStatistic.getvCoresMillisTotal()) / (1000);
        }
        int jobNumbers = usageStatistics.size();
        jobReportJson.put("user", usageStatistics.get(0).getUser()); // TODO(hans-permana, 20170116): should generate the report per user
        jobReportJson.put("jobsProcessed", String.valueOf(jobNumbers));
        jobReportJson.put("totalFileWritingMb", getFormattedNumber(totalFileWriting));
        jobReportJson.put("totalFileReadingMb", getFormattedNumber(totalFileReading));
        jobReportJson.put("totalCpuTimeSpent", getElapsedTime(totalCpuTime));
        jobReportJson.put("totalMemoryUsedMbs", getFormattedNumber(totalMemoryUsed));
        jobReportJson.put("totalVcoresUsed", getFormattedNumber(totalVCoresUsed));
        double cpuPrice = PriceCalculator.getCpuPrice(totalVCoresUsed);
        double memoryPrice = PriceCalculator.getMemoryPrice(totalMemoryUsed);
        double diskPrice = PriceCalculator.getDiskPrice(totalFileWriting + totalFileReading);
        jobReportJson.put("cpuUsagePrice", String.valueOf(cpuPrice));
        jobReportJson.put("memoryUsagePrice", String.valueOf(memoryPrice));
        jobReportJson.put("diskUsageprice", String.valueOf(diskPrice));
        jobReportJson.put("totalPrice", String.valueOf(cpuPrice + memoryPrice + diskPrice));
        return jobReportJson;
    }

    private List<String> getMultiJobReportContents(List<UsageStatistic> usageStatistics) {
        List<String> jobReport = new ArrayList<>();
        long totalFileWriting = 0;
        long totalFileReading = 0;
        long totalCpuTime = 0;
        long totalMemoryUsed = 0;
        long totalVCoresUsed = 0;
        for (UsageStatistic usageStatistic : usageStatistics) {
            totalFileWriting += (usageStatistic.getFileBytesWritten() + usageStatistic.getHdfsBytesWritten()) / (1024 * 1024);
            totalFileReading += (usageStatistic.getFileBytesRead() + usageStatistic.getHdfsBytesRead()) / (1024 * 1024);
            totalCpuTime += usageStatistic.getCpuMilliseconds();
            totalMemoryUsed += (usageStatistic.getMbMillisTotal()) / (1000);
            totalVCoresUsed += (usageStatistic.getvCoresMillisTotal()) / (1000);
        }
        int jobNumbers = usageStatistics.size();
        jobReport.add("Usage statistic for user $USER in $MONTH $YEAR"); // TODO(hans-permana, 20170116): should generate the report per user
        jobReport.add("");
        jobReport.add("Jobs processed : " + jobNumbers);
        jobReport.add("Total file writing (MB) : " + getFormattedNumber(totalFileWriting));
        jobReport.add("Total file reading (MB) : " + getFormattedNumber(totalFileReading));
        jobReport.add("Total CPU time spent : " + getElapsedTime(totalCpuTime));
        jobReport.add("Total Memory used (MB s) :  " + getFormattedNumber(totalMemoryUsed));
        jobReport.add("Total vCores used (vCore s) :  " + getFormattedNumber(totalVCoresUsed));
        jobReport.add("");
        jobReport.add("");
        jobReport.add("Price breakdown");
        jobReport.add("");
        double cpuPrice = PriceCalculator.getCpuPrice(totalVCoresUsed);
        double memoryPrice = PriceCalculator.getMemoryPrice(totalMemoryUsed);
        double diskPrice = PriceCalculator.getDiskPrice(totalFileWriting + totalFileReading);
        jobReport.add("CPU usage price = (Total vCores used) x € 0.0013 = € " +
                              cpuPrice);
        jobReport.add("Memory usage price = (Total Memory used) x € 0.00022 = € " +
                              memoryPrice);
        jobReport.add("Disk space usage price = (Total file writing GB + Total file reading GB) x € 0.011 = € " +
                              diskPrice);
        jobReport.add("");
        jobReport.add("Total = € " + (cpuPrice + memoryPrice + diskPrice));
        return jobReport;
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
        DateFormat df = DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, new Locale("de", "DE"));
        return df.format(startTime);
    }
}
