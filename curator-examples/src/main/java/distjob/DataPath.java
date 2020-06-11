package distjob;

public class DataPath {

    private String jobName;

    //   /distjob/jobName/instance
    private  String instancePath ;

    private String leaderPath;

    // /distjob/jobName/shardingData
    private  String shardingDataPath;

    // /distjob/jobName/shardingCount

    // /distjob/jobName/shardingCount
    private String shardingCountPath;

    // /distjob/jobName/suggest/0
    private  String shardSuggestPath;

    private  String shardLockPath;


    public DataPath(String jobName){
        this.jobName = jobName;
        instancePath = String.format("/distjob/%s/instance", jobName);
        leaderPath = String.format("/distjob/%s/leader", jobName);
        shardingDataPath = String.format("/distjob/%s/shardingData", jobName);
        shardingCountPath = String.format("/distjob/%s/shardingCount", jobName);
        shardSuggestPath = String.format("/distjob/%s/suggest/", jobName);
        shardLockPath = String.format("/distjob/%s/lock/", jobName);
    }

    public String getJobName() {
        return jobName;
    }
    public String getInstancePath() {
        return instancePath;
    }

    public String getLeaderPath() {
        return leaderPath;
    }

    public String getShardSuggestPath() {
        return shardSuggestPath;
    }

    public String getShardingDataPath() {
        return shardingDataPath;
    }

    public String getShardingCountPath() {
        return shardingCountPath;
    }


    public String getShardSuggestPath(int item) {
        return shardSuggestPath + item;
    }

    public String getShardLockPath(int item) {
        return shardLockPath + item;
    }

}
