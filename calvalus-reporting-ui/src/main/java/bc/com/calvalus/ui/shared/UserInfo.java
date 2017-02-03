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
    private String totalMaps;

    public UserInfo() {
    }

    public UserInfo(String user, String jobsProcessed, String totalFileReadingMb, String totalFileWritingMb, String totalMemoryUsedMbs, String totalCpuTimeSpent, String totalMaps) {
        this.user = user;
        this.jobsProcessed = jobsProcessed;
        this.totalFileReadingMb = totalFileReadingMb;
        this.totalFileWritingMb = totalFileWritingMb;
        this.totalMemoryUsedMbs = totalMemoryUsedMbs;
        this.totalCpuTimeSpent = totalCpuTimeSpent;
        this.totalMaps = totalMaps;
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

    public String getTotalMaps() {
        return totalMaps;
    }
}
