package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The bury command puts a job into the &quot;buried&quot; state. Buried jobs are put into a
 * FIFO linked list and will not be touched by the server again until a client
 * kicks them with the &quot;kick&quot; command.
 * 
 * The bury command looks like this:
 * 
 * bury &lt;id&gt; &lt;pri&gt;\r\n
 * 
 *  - &lt;id&gt; is the job id to release.
 * 
 *  - &lt;pri&gt; is a new priority to assign to the job.
 * 
 * There are two possible responses:
 * 
 *  - &quot;BURIED\r\n&quot; to indicate success.
 * 
 *  - &quot;NOT_FOUND\r\n&quot; if the job does not exist or is not reserved by the client.
 * 
 * </pre>
 */
public class BuryCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        long id = ArgParserUtils.toLong(msg.getArgs()[0]);
        long pri = ArgParserUtils.toLong(msg.getArgs()[1]);

        boolean released = client.bury(id, pri);

        if (released) {
            return new BeanstalkMessage(
                    BeanstalkProtocol.BURIED.getCommandName(), null, -1);
        } else {
            return new BeanstalkMessage(
                    BeanstalkProtocol.NOT_FOUND.getCommandName(), null, -1);
        }

    }

}
