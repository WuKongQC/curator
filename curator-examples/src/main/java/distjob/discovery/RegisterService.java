package distjob.discovery;

import discovery.InstanceDetails;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author wangjie
 * @date 2020/6/10 17:06
 */
public class RegisterService implements Closeable {


    private final ServiceDiscovery<String> serviceDiscovery;
    private final ServiceInstance<String> thisInstance;

    public RegisterService(CuratorFramework client, String path, String serviceName,  String jobInstanceId) throws Exception
    {
        // in a real application, you'd have a convention of some kind for the URI layout
        UriSpec uriSpec = new UriSpec("{scheme}://foo.com:{port}");

        thisInstance = ServiceInstance.<String>builder()
                .name(serviceName)
                .payload(jobInstanceId)
                .uriSpec(uriSpec)
                .build();

        // if you mark your payload class with @JsonRootName the provided JsonInstanceSerializer will work
        JsonInstanceSerializer<String> serializer = new JsonInstanceSerializer<String>(String.class);

        serviceDiscovery = ServiceDiscoveryBuilder.builder(String.class)
                .client(client)
                .basePath(path)
                .serializer(serializer)
                .thisInstance(thisInstance)
                .build();
    }

    public ServiceInstance<String> getThisInstance()
    {
        return thisInstance;
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
