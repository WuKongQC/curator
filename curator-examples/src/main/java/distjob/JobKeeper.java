package distjob;

import distjob.discovery.FinderService;
import distjob.discovery.RegisterService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

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

    //WorkKeeper workKeeper;

    WorkLeader wordLeader;

    EventBus eventBus;

    CuratorCache shardDataCache;

    SimpleJob job;


    JobKeeper(String jobName, String connectString){
        this.jobName = jobName;
        dataPath = new DataPath(jobName);

        this.connectString = connectString;
        this.job = new OneJob();
    }




    public void init() {

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

        initData(12, client);

        initService();



        wordLeader = new WorkLeader(client, dataPath, job);




        client.getConnectionStateListenable().addListener((a, b)->{
                    if(b == ConnectionState.SUSPENDED || b == ConnectionState.LOST){
                        eventBus.setEvent(EventBus.EventType.EventType_ReFreash);
                    }
        });

        eventBus.setEvent(EventBus.EventType.EventType_ReShard);
        workLoop();
    }


    public void workLoop() {
        while(true){
            try {
                Thread.sleep(5*10000);

                Set<EventBus.EventType> events =  eventBus.aqiureEvent();

                for(EventBus.EventType event: events){

                    switch (event){
                        case EventType_ReShard:
                            processReshard();
                            break;
                        case EventType_ReFreash:
                            return;
                        default:
                            break;

                    }
                }


            }catch (InterruptedException ex){
                System.out.println(" sleep:" + ex);
            }
        }

    }


    void processReshard() {
        boolean suc = false;
        while(!suc){
            try{
                int shardSum = Integer.parseInt(new String(client.getData().forPath(dataPath.getShardingCountPath())));

                Set<Integer> shardItems = new HashSet<>();

                for(int i = 0; i < shardSum; ++i){
                    String node = new String(client.getData().forPath(dataPath.getShardSuggestPath(i)));
                    if(jobInstanceId.equals(node)){
                        shardItems.add(i);
                    }
                }

                wordLeader.refresh(shardSum,shardItems);
                suc = true;
            }catch (Throwable th){
                System.out.println("th:" + th);
            }
        }



    }



    void initData(int shardSum, CuratorFramework client){
        int tryTime = 0;
        boolean res = false;
        while(!res){
            try{
                tryTime++;
                System.out.println( "try init data:" + tryTime);
                res = initDataOnce(shardSum, client);
            }catch (Throwable ex){
                System.out.println( "initData:" + ex);
                try {
                    Thread.sleep(10*1000);
                }catch (Exception ee){

                }

            }


        }
    }

    boolean initDataOnce(int shardSum, CuratorFramework client) throws  Exception{


        Stat st = client.checkExists().forPath(dataPath.getInstancePath());
        if(st == null){
            try{
                client.create().creatingParentContainersIfNeeded().forPath(dataPath.getInstancePath(),
                        String.valueOf(shardSum).getBytes());
            }catch (KeeperException.NodeExistsException existsException){

            }
        }

        for(int item = 0; item < shardSum; ++item){
            Stat st1 = client.checkExists().forPath(dataPath.getShardSuggestPath(item));
            if(st1 == null){
                try{
                    client.create().creatingParentContainersIfNeeded().forPath(dataPath.getShardSuggestPath(item));
                }catch (KeeperException.NodeExistsException existsException){

                }
            }

            st1 = client.checkExists().forPath(dataPath.getShardLockPath(item));
            if(st1 == null){
                try{
                    client.create().creatingParentContainersIfNeeded().forPath(dataPath.getShardLockPath(item));
                }catch (KeeperException.NodeExistsException existsException){

                }
            }
        }

       st = client.checkExists().forPath(dataPath.getShardingDataPath());
        if(st == null){
            try{
                client.create().creatingParentContainersIfNeeded().forPath(dataPath.getShardingDataPath(),
                        String.valueOf(shardSum).getBytes());
            }catch (KeeperException.NodeExistsException existsException){

            }
        }

        st = client.checkExists().forPath(dataPath.getLeaderPath());
        if(st == null){
            try{
                client.create().creatingParentContainersIfNeeded().forPath(dataPath.getLeaderPath());
            }catch (KeeperException.NodeExistsException existsException){

            }
        }

         st = client.checkExists().forPath(dataPath.getShardingCountPath());
        if(st == null){
            try{
                client.create().creatingParentContainersIfNeeded().forPath(dataPath.getShardingCountPath(),
                         String.valueOf(shardSum).getBytes());
            }catch (KeeperException.NodeExistsException existsException){

            }

        }else {
            while(true){
                int v = st.getVersion();
                int shardVal = Integer.parseInt(new String(client.getData().forPath(dataPath.getShardingCountPath())));
                if( shardSum > shardVal){
                    try {
                        System.out.println("set ShardingCount:" + shardSum);
                        client.setData().withVersion(v).forPath(dataPath.getShardingCountPath(), String.valueOf(shardSum).getBytes());
                        break;
                    }catch (KeeperException.BadVersionException bv){
                        client.getData().storingStatIn(st).forPath(dataPath.getShardingCountPath());
                    }
                }else{
                    break;
                }
            }

        }

        return true;

    }


    void initService(){
        int tryTime = 0;
        while(!initServiceOnce()){
            try{
                Thread.sleep(10*1000);
            }catch (Exception ex){

            }
            System.out.println("try agine:" + tryTime);
        }
    }

    boolean initServiceOnce(){
        if(finderService == null){
            try{
                finderService = new FinderService(client, dataPath.getInstancePath(), jobName);
                finderService.start();
            }catch (Exception ex){
                if(finderService != null){
                    CloseableUtils.closeQuietly(finderService);
                    finderService = null;
                }

                return false;
            }
        }

        if(registerService == null){
            try{
                registerService = new RegisterService(client, dataPath.getInstancePath(), jobName, jobInstanceId);
                registerService.start();
            }catch (Exception ex){
                if(registerService != null){
                    CloseableUtils.closeQuietly(registerService);
                    registerService = null;
                }
                return false;
            }
        }

        if(leaderService == null){
            try{
                leaderService = new LeaderService(client, finderService, dataPath, jobName);
                leaderService.start();
            }catch (Exception ex){
                if(leaderService != null){
                    CloseableUtils.closeQuietly(leaderService);
                    leaderService = null;
                }
                return false;
            }
        }


        if(shardDataCache == null){
            try{
                CuratorCacheListener listener = CuratorCacheListener.builder().forCreatesAndChanges((o, n)->{
                    System.out.println("listener resharded!");
                    eventBus.setEvent(EventBus.EventType.EventType_ReShard);
                }).build();

                shardDataCache = CuratorCache.build(client, dataPath.getShardingDataPath());
                shardDataCache.listenable().addListener(listener);
                shardDataCache.start();
            }catch (Exception ex){
                if(shardDataCache != null){
                    CloseableUtils.closeQuietly(shardDataCache);
                    shardDataCache = null;
                }
                return false;
            }

        }

        try{
            List<ServiceInstance<String>> services =  finderService.listInstances(jobName);
            System.out.println("list servers start:");
            for(ServiceInstance<String> ss:services){
                System.out.println("server:" + ss.getAddress() + ":" + ss.getPort());
            }
            System.out.println("list servers end");
        }catch (Exception ex){
            System.out.println("ex:" + ex);
        }

        return true;
    }







    public void close() throws Exception {

        if(wordLeader != null){
            wordLeader.close();
            wordLeader = null;
        }


        if(shardDataCache != null){
            CloseableUtils.closeQuietly(shardDataCache);
            shardDataCache = null;
        }
        if(leaderService != null){
            CloseableUtils.closeQuietly(leaderService);
            leaderService = null;
        }

        if(registerService != null){
            CloseableUtils.closeQuietly(registerService);
            registerService = null;
        }

        if(finderService != null){
            CloseableUtils.closeQuietly(finderService);
            finderService = null;
        }
        if(client != null){
            CloseableUtils.closeQuietly(client);
            client = null;
        }


    }




    public static void main(String[] args) throws Exception{


        JobKeeper jk = new JobKeeper("OneJob", "172.28.22.7:2181");

        jk.init();

        Thread.sleep(60*1000);
        jk.close();



    }
}
