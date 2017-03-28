package bc.com.calvalus.ui.shared;


import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * @author muhammad.bc.
 */
public class UserInfo implements IsSerializable, Comparable<UserInfo> {
    private String user;
    private String jobsInDate;
    private String jobsInQueue;
    private String jobsProcessed;
    private String totalFileReadingMb;
    private String totalFileWritingMb;
    private String totalMemoryUsedMbs;
    private String totalCpuTimeSpent;
    private String totalMap;

    public UserInfo() {
    }

    public UserInfo(String jobsInDate, String jobsInQueue, String user, String jobsProcessed, String totalFileReadingMb, String totalFileWritingMb, String totalMemoryUsedMbs, String totalCpuTimeSpent, String totalMap) {
        this.jobsInDate = jobsInDate;
        this.jobsInQueue = jobsInQueue;
        this.user = user;
        this.jobsProcessed = jobsProcessed;
        this.totalFileReadingMb = totalFileReadingMb;
        this.totalFileWritingMb = totalFileWritingMb;
        this.totalMemoryUsedMbs = totalMemoryUsedMbs;
        this.totalCpuTimeSpent = totalCpuTimeSpent;
        this.totalMap = totalMap;
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

    public String getTotalMap() {
        return totalMap;
    }

    public String getJobsInDate() {
        return jobsInDate;
    }

    public String getJobsInQueue() {
        return jobsInQueue;
    }

    @Override
    public int compareTo(UserInfo o) {
        return (o == null || o.user == null) ? -1 : -o.user.compareTo(user);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof UserInfo) {
            return user == ((UserInfo) o).user;
        }
        return false;
    }
}
