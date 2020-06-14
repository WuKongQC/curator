package distjob;

import distjob.discovery.FinderService;
import distjob.discovery.RegisterService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceInstance;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * @author wangjie
 * @date 2020/6/10 16:30
 */
public class JobKeeper {

    private static final String DELIMITER = "@-@";

    private String jobInstanceId = IpUtils.getIp() + DELIMITER + ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

    private  DataPath dataPath;

    private  String jobName ;

    // ZooKeeper 服务地址, 单机格式为:= "172.28.22.7:2181"
    // 集群格式为:(127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183)
    private String connectString;

    // Curator 客户端重试策略
    private RetryPolicy retry;

    // Curator 客户端对象
    private CuratorFramework client;


    FinderService finderService;

    RegisterService registerService;

    LeaderService leaderService;

    CuratorCache reshardData;

    WorkKeeper workKeeper;

    EventBus eventBus;

    SimpleJob job;


    JobKeeper(String jobName, String connectString){
        this.jobName = jobName;
        dataPath = new DataPath(jobName);

        this.connectString = connectString;
        this.job = new OneJob();
    }




    public void init() throws Exception {

        eventBus = new EventBus();

        // 设置 ZooKeeper 服务地址为本机的 2181 端口
        //connectString = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
        // 重试策略
        // 初始休眠时间为 1000ms, 最大重试次数为 3
        retry = new ExponentialBackoffRetry(1000, 3);
        // 创建一个客户端, 60000(ms)为 session 超时时间, 15000(ms)为链接超时时间
        client = CuratorFrameworkFactory.newClient(connectString, 60000, 15000, retry);

        // 创建会话
        client.start();


        finderService = new FinderService(client, dataPath.getInstancePath(), jobName);
        finderService.start();

        registerService = new RegisterService(client, dataPath.getInstancePath(), jobName, jobInstanceId);
        registerService.start();

        leaderService = new LeaderService(client, finderService, dataPath, jobName);
        leaderService.start();

        {
            reshardData = CuratorCache.build(client, dataPath.getShardingDataPath());
            CuratorCacheListener listener = CuratorCacheListener.builder().
                    forCreatesAndChanges((a, b)->eventBus.setEvent(EventBus.EventType.EventType_ReFreash)).build();
            reshardData.listenable().addListener(listener);
        }



        workKeeper = new WorkKeeper(client, dataPath, job);



        List<ServiceInstance<String>> services =  finderService.listInstances(jobName);
        System.out.println("list servers start:");
        for(ServiceInstance<String> ss:services){
            System.out.println("server:" + ss.getAddress() + ":" + ss.getPort());
        }
        System.out.println("list servers end");
    }


    public void workLoop() throws Exception{
        while(true){

            Thread.sleep(5*10000);

            Set<EventBus.EventType> events =  eventBus.aqiureEvent();

            for(EventBus.EventType event: events){

                switch (event){
                    case EventType_ReShard:
                        processReshard();
                        break;
                        default:
                            break;

                }
            }
        }


    }


    void processReshard() throws Exception{
        int shardSum = Integer.parseInt(new String(client.getData().forPath(dataPath.getShardingCountPath())));

        Set<Integer> shardItems = new HashSet<>();

        for(int i = 0; i < shardSum; ++i){
            String node = new String(client.getData().forPath(dataPath.getShardSuggestPath(i)));
            if(jobInstanceId.equals(node)){
                shardItems.add(i);
            }
        }

        workKeeper.refresh(shardSum,shardItems);

    }







    public void close() throws Exception {
        if(registerService != null){
            registerService.close();
        }

        if(finderService != null){
            finderService.close();
        }
        if(client != null){
            client.close();
        }
    }




    public static void main(String[] args) throws Exception{


        JobKeeper jk = new JobKeeper("distJob", "172.28.22.7:2181");

        jk.init();

        Thread.sleep(1000);
        jk.close();



    }
}
