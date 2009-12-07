package com.xedom.beanstalkj.protocol;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.local.BTServer;

public class BeanstalkServerPipelineFactory implements ChannelPipelineFactory {

    private BTServer server = new BTServer();

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("encoder", new BeanstalkMessageEncoder());
        pipeline.addLast("decoder", new BeanstalkMessageDecoder());
        pipeline.addLast("executor", new ExecutionHandler(
                new OrderedMemoryAwareThreadPoolExecutor(16, 1048576, 1048576)));

        BTClient client = new BTClient(server);
        System.out.println("Client created");
        server.connect(client);

        pipeline.addLast("handler", new BeanstalkServerHandler(client));
        return pipeline;
    }

}
