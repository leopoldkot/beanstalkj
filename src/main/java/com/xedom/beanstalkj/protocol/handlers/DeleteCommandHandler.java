package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The release command puts a reserved job back into the ready queue (and marks
 * its state as &quot;ready&quot;) to be run by any client. It is normally used when the
 * job fails because of a transitory error. It looks like this:
 * 
 * release &lt;id&gt; &lt;pri&gt; &lt;delay&gt;\r\n
 * 
 * - &lt;id&gt; is the job id to release.
 * 
 * - &lt;pri&gt; is a new priority to assign to the job.
 * 
 * - &lt;delay&gt; is an integer number of seconds to wait before putting the job in
 * the ready queue. The job will be in the &quot;delayed&quot; state during this time.
 * 
 * The client expects one line of response, which may be:
 * 
 * - &quot;RELEASED\r\n&quot; to indicate success.
 * 
 * - &quot;BURIED\r\n&quot; if the server ran out of memory trying to grow the priority
 * queue data structure.
 * 
 * - &quot;NOT_FOUND\r\n&quot; if the job does not exist or is not reserved by the client.
 * </pre>
 */
public class DeleteCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        long id = ArgParserUtils.toLong(msg.getArgs()[0]);

        boolean delete = client.delete(id);
        if (delete) {
            return new BeanstalkMessage(
                    BeanstalkProtocol.DELETED.getCommandName(), null, -1);
        } else {
            return new BeanstalkMessage(
                    BeanstalkProtocol.NOT_FOUND.getCommandName(), null, -1);
        }

    }

}
