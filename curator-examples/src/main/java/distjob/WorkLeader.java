package distjob;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.utils.CloseableUtils;

import java.io.Closeable;
import java.util.Set;

/**
 * @author wangjie
 * @date 2020/6/16 15:43
 */
public class WorkLeader {

    CuratorFramework client;
    DataPath dataPath;

    SimpleJob job;

    WorkLeader.WorkNode[]  workNodes;

    public WorkLeader(CuratorFramework client, DataPath dataPath, SimpleJob job){
        this.client = client;
        this.dataPath = dataPath;
        this.job = job;
    }

    public void refresh(int shardSum, Set<Integer> allocItem){

        WorkLeader.WorkNode[] newNodeList  = new WorkLeader.WorkNode[shardSum];
        for(int i = 0; i < workNodes.length; ++i){
            if(i < newNodeList.length){
                newNodeList[i] = workNodes[i];
            }else {
                workNodes[i].close();
            }
        }
        workNodes = newNodeList;

        for(int i = 0; i <workNodes.length; ++i){
            if(allocItem.contains(i)){
                if(workNodes[i] == null){
                    workNodes[i] = new WorkLeader.WorkNode(client, i, dataPath.getShardLockPath(i), job);
                    workNodes[i].start();
                }
            }else{
                if(workNodes[i] != null){
                    workNodes[i].close();
                    workNodes[i] = null;
                }
            }
        }
    }

    public void close(){
        for(int i = 0; i < workNodes.length; ++i){
            if( workNodes[i] != null){
                workNodes[i].close();
            }
        }
    }


    private static class WorkNode extends LeaderSelectorListenerAdapter implements Closeable {
        int shard;
        //private int status; //0:获取锁，1:结束
       // private Thread workThread;
        LeaderSelector leaderSelector;
        private SimpleJob job;

        public WorkNode(CuratorFramework client, int shard, String path,  SimpleJob job){
            this.shard = shard;
            leaderSelector = new LeaderSelector(client, path, this);

            // for most cases you will want your instance to requeue when it relinquishes leadership
            leaderSelector.autoRequeue();
            this.job = job;
        }


        public void start() {
            leaderSelector.start();
        }

        @Override
        public void close(){
            //leaderSelector.close();
            CloseableUtils.closeQuietly(leaderSelector);
        }


        @Override
        public void takeLeadership(CuratorFramework client) throws Exception{
            while(true){
                job.execute(shard);
            }
        }
    }
}
