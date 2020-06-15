package distjob;

import distjob.discovery.FinderService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.List;
import java.util.Set;

/**
 * @author wangjie
 * @date 2020/6/11 10:59
 */
public class WorkKeeper {
    CuratorFramework client;
    DataPath dataPath;

    SimpleJob job;

    WorkNode[]  workNodes;

    public WorkKeeper(CuratorFramework client, DataPath dataPath, SimpleJob job){
        this.client = client;
        this.dataPath = dataPath;
        this.job = job;
    }


    public void refresh(int shardSum, Set<Integer> allocItem){

        WorkNode[] newNodeList  = new WorkNode[shardSum];
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
                    workNodes[i] = new WorkNode(i, new InterProcessMutex(client, dataPath.getShardLockPath(i)), job);
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






    private static class WorkNode implements  Runnable{
        int shard;
        private int status; //0:获取锁，1:结束
        private Thread workThread;
        private  InterProcessMutex lock;
        private SimpleJob job;

            public WorkNode(int shard, InterProcessMutex lock,  SimpleJob job){
                this.shard = shard;
                this.lock = lock;
                this.job = job;
                status = 0;
            }

           public void start(){
               workThread = new Thread(this, this + "-" + shard);
               workThread.start();
           }


           public void close(){
               status = 1;
               workThread.interrupt();
           }


          public  void run(){
                while(true){
                    if(status == 1){
                        return;
                    }
                    try{
                        lock.acquire();
                        job.execute(shard);
                        lock.release();
                    }catch (InterruptedException interrupted){
                        if(status == 1){
                            return;
                        }
                    }catch (Throwable ex){
                        if(status == 1){
                            return;
                        }
                    }finally {
                        try {
                            lock.release();
                        }catch (Exception ex){

                        }
                    }
                }
            }
    }
}
