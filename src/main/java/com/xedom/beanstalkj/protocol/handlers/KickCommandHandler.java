package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The kick command applies only to the currently used tube. It moves jobs into
 * the ready queue. If there are any buried jobs, it will only kick buried jobs.
 * Otherwise it will kick delayed jobs. It looks like:
 * 
 * kick &lt;bound&gt;\r\n
 * 
 *  - &lt;bound&gt; is an integer upper bound on the number of jobs to kick. The server
 *    will kick no more than &lt;bound&gt; jobs.
 * 
 * The response is of the form:
 * 
 * KICKED &lt;count&gt;\r\n
 * 
 *  - &lt;count&gt; is an integer indicating the number of jobs actually kicked.
 * </pre>
 */
public class KickCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        int bound = ArgParserUtils.toInt(msg.getArgs()[0]);

        int count = client.kick(bound);

        String[] args = new String[] { Integer.toString(count) };

        return new BeanstalkMessage(BeanstalkProtocol.KICKED.getCommandName(),
                args, -1);
    }

}
