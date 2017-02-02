package bc.com.calvalus.ui.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * @author muhammad.bc.
 */
public class UserInfo implements IsSerializable {
    private String user;
    private String jobsProcessed;
    private String totalFileReadingMb;
    private String totalFileWritingMb;
    private String totalMemoryUsedMbs;
    private String totalCpuTimeSpent;
    private String totalVcoresUsed;
    private String string;

    public UserInfo() {
    }

    public UserInfo(String user, String jobsProcessed, String totalFileReadingMb, String totalFileWritingMb, String totalMemoryUsedMbs, String totalCpuTimeSpent, String totalVcoresUsed) {
        this.user = user;
        this.jobsProcessed = jobsProcessed;
        this.totalFileReadingMb = totalFileReadingMb;
        this.totalFileWritingMb = totalFileWritingMb;
        this.totalMemoryUsedMbs = totalMemoryUsedMbs;
        this.totalCpuTimeSpent = totalCpuTimeSpent;
        this.totalVcoresUsed = totalVcoresUsed;
    }

    public String getUser() {
        return user;
    }

    public String getJobsProcessed() {
        return jobsProcessed;
    }

    public String getTotalFileReadingMb() {
        return totalFileReadingMb;
    }


    public String getTotalFileWritingMb() {
        return totalFileWritingMb;
    }

    public String getTotalMemoryUsedMbs() {
        return totalMemoryUsedMbs;
    }

    public String getTotalCpuTimeSpent() {
        return totalCpuTimeSpent;
    }

    public String getTotalVcoresUsed() {
        return totalVcoresUsed;
    }



}
