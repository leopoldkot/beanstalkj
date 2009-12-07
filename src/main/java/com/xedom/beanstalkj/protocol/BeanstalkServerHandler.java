package com.xedom.beanstalkj.protocol;

import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.error.BadFormatException;
import com.xedom.beanstalkj.protocol.handlers.BuryCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.CommandHandler;
import com.xedom.beanstalkj.protocol.handlers.DeleteCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.IgnoreCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.KickCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.ListTubeUsedCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.ListTubesCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.ListTubesWatchedCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.PeekBuriedCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.PeekCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.PeekDelayedCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.PeekReadyCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.PutCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.ReleaseCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.ReserveCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.ReserveWithTimeoutCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.StatsCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.StatsJobCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.StatsTubeCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.TouchCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.UseCommandHandler;
import com.xedom.beanstalkj.protocol.handlers.WatchCommandHandler;

@ChannelPipelineCoverage("one")
public class BeanstalkServerHandler extends SimpleChannelUpstreamHandler {

    private Map<String, CommandHandler> handlers = new HashMap<String, CommandHandler>();

    private final BTClient client;

    public BeanstalkServerHandler(BTClient client) {
        super();

        this.client = client;
        initHandlers();

    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent evt) {
        BeanstalkMessage msg = (BeanstalkMessage) evt.getMessage();
        BeanstalkMessage resp = null;

        CommandHandler handler = findHandler(msg);
        if (handler == null) {
            resp = BeanstalkProtocol.BAD_FORMAT_MSG;
        } else {
            try {
                resp = handler.handleCommand(client, msg);
            } catch (BadFormatException e) {
                e.printStackTrace();
                resp = BeanstalkProtocol.BAD_FORMAT_MSG;
            }
        }

        evt.getChannel().write(resp);
    }

    private void initHandlers() {

        handlers.put(BeanstalkProtocol.BURY.getCommandName(),
                new BuryCommandHandler());
        handlers.put(BeanstalkProtocol.DELETE.getCommandName(),
                new DeleteCommandHandler());
        handlers.put(BeanstalkProtocol.IGNORE.getCommandName(),
                new IgnoreCommandHandler());
        handlers.put(BeanstalkProtocol.KICK.getCommandName(),
                new KickCommandHandler());
        handlers.put(BeanstalkProtocol.LIST_TUBES.getCommandName(),
                new ListTubesCommandHandler());
        handlers.put(BeanstalkProtocol.LIST_TUBES_WATCHED.getCommandName(),
                new ListTubesWatchedCommandHandler());
        handlers.put(BeanstalkProtocol.LIST_TUBE_USED.getCommandName(),
                new ListTubeUsedCommandHandler());
        handlers.put(BeanstalkProtocol.PEEK_BURIED.getCommandName(),
                new PeekBuriedCommandHandler());
        handlers.put(BeanstalkProtocol.PEEK.getCommandName(),
                new PeekCommandHandler());
        handlers.put(BeanstalkProtocol.PEEK_DELAYED.getCommandName(),
                new PeekDelayedCommandHandler());
        handlers.put(BeanstalkProtocol.PEEK_READY.getCommandName(),
                new PeekReadyCommandHandler());
        handlers.put(BeanstalkProtocol.PUT.getCommandName(),
                new PutCommandHandler());

        handlers.put(BeanstalkProtocol.RELEASE.getCommandName(),
                new ReleaseCommandHandler());

        handlers.put(BeanstalkProtocol.RESERVE.getCommandName(),
                new ReserveCommandHandler());
        handlers.put(BeanstalkProtocol.RESERVE_WITH_TIMEOUT.getCommandName(),
                new ReserveWithTimeoutCommandHandler());

        handlers.put(BeanstalkProtocol.STATS.getCommandName(),
                new StatsCommandHandler());
        handlers.put(BeanstalkProtocol.STATS_JOB.getCommandName(),
                new StatsJobCommandHandler());
        handlers.put(BeanstalkProtocol.STATS_TUBE.getCommandName(),
                new StatsTubeCommandHandler());
        handlers.put(BeanstalkProtocol.TOUCH.getCommandName(),
                new TouchCommandHandler());

        handlers.put(BeanstalkProtocol.USE.getCommandName(),
                new UseCommandHandler());

        handlers.put(BeanstalkProtocol.WATCH.getCommandName(),
                new WatchCommandHandler());

        // ReleaseCommandHandler

    }

    private CommandHandler findHandler(BeanstalkMessage msg) {
        return handlers.get(msg.getCommand());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        e.getCause().printStackTrace();

        Channel ch = e.getChannel();
        ch.close();
    }

}