/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package distjob;

import distjob.discovery.FinderService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.x.discovery.ServiceInstance;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An example leader selector client. Note that {@link LeaderSelectorListenerAdapter} which
 * has the recommended handling for connection state issues
 */
public class LeaderService extends LeaderSelectorListenerAdapter implements Closeable
{



    //   /distjob/jobName/instance
    private  DataPath dataPath;


    FinderService finderService;

    private final String jobName;
    private final LeaderSelector leaderSelector;
    private final AtomicInteger leaderCount = new AtomicInteger();

    public LeaderService(CuratorFramework client, FinderService finderService, DataPath dataPath, String jobName)
    {
        this.jobName = jobName;
        this.dataPath = dataPath;
        this.finderService = finderService;

        // create a leader selector using the given path for management
        // all participants in a given leader selection must use the same path
        // ExampleClient here is also a LeaderSelectorListener but this isn't required
        leaderSelector = new LeaderSelector(client, dataPath.getLeaderPath(), this);

        // for most cases you will want your instance to requeue when it relinquishes leadership
        leaderSelector.autoRequeue();
    }

    public void start() throws IOException
    {
        // the selection for this instance doesn't start until the leader selector is started
        // leader selection is done in the background so this call to leaderSelector.start() returns immediately
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException
    {
        leaderSelector.close();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception
    {
        // we are now the leader. This method should not return until we want to relinquish leadership


        System.out.println(jobName + " is now the leader. Waiting " + 5 + " seconds...");
        System.out.println(jobName + " has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
        try
        {


            Thread.sleep(TimeUnit.SECONDS.toMillis(5));


            List<ServiceInstance<String>> instances  = finderService.listInstances(jobName);

            List<String> usedInst = new LinkedList<>();
            for(ServiceInstance<String> inst :instances){
                usedInst.add(inst.getAddress()+":" + inst.getPort());
            }
            usedInst.sort((Comparator.naturalOrder()));

            StringBuilder dataBuilder = new StringBuilder();
            for(String ss:usedInst){
                dataBuilder.append("$" +  ss);
            }
            String shardCountStr =  new String( client.getData().forPath(dataPath.getShardingCountPath()));
            dataBuilder.append( "&" +shardCountStr);


            String lastShardData = new String( client.getData().forPath(dataPath.getShardingDataPath()));

            if(usedInst.size() != 0 && !lastShardData.equals(dataBuilder.toString())){
                int sc = Integer.parseInt(shardCountStr);


                for(int i = 0; i < sc; ++i){
                    client.setData().forPath(dataPath.getShardSuggestPath(i), usedInst.get(i%usedInst.size()).getBytes());
                }
            }


        }
        catch ( InterruptedException e )
        {
            System.err.println(jobName + " was interrupted.");
            Thread.currentThread().interrupt();
        }
        finally
        {
            System.out.println(jobName + " relinquishing leadership.\n");
        }
    }
}
