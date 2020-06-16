package distjob.discovery;

import discovery.InstanceDetails;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * @author wangjie
 * @date 2020/6/10 17:08
 */
public class FinderService   implements Closeable {

    private final ServiceDiscovery<String> serviceDiscovery;


    public FinderService(CuratorFramework client, String path, String serviceName) {

        JsonInstanceSerializer<String> serializer = new JsonInstanceSerializer<String>(String.class);
        serviceDiscovery = ServiceDiscoveryBuilder.builder(String.class).client(client).basePath(path).serializer(serializer).build();

    }


    public List<ServiceInstance<String>>  listInstances(String serviceName) throws Exception
    {
        // This shows how to query all the instances in service discovery
        List<ServiceInstance<String>> services = new LinkedList<>();
        try
        {


            Collection<ServiceInstance<String>> instances = serviceDiscovery.queryForInstances(serviceName);
            System.out.println(serviceName);
            for ( ServiceInstance<String> instance : instances )
            {
                services.add(instance);
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(serviceDiscovery);
        }

        return services;
    }




    public void start() throws Exception
    {
        serviceDiscovery.start();
    }

    @Override
    public void close() throws IOException
    {
        CloseableUtils.closeQuietly(serviceDiscovery);
    }
}
