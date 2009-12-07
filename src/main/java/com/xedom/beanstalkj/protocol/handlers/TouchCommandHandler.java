package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The &quot;touch&quot; command allows a worker to request more time to work on a job.
 * This is useful for jobs that potentially take a long time, but you still want
 * the benefits of a TTR pulling a job away from an unresponsive worker.  A worker
 * may periodically tell the server that it's still alive and processing a job
 * (e.g. it may do this on DEADLINE_SOON).
 * 
 * The touch command looks like this:
 * 
 * touch &lt;id&gt;\r\n
 * 
 *  - &lt;id&gt; is the ID of a job reserved by the current connection.
 * 
 * There are two possible responses:
 * 
 *  - &quot;TOUCHED\r\n&quot; to indicate success.
 * 
 *  - &quot;NOT_FOUND\r\n&quot; if the job does not exist or is not reserved by the client.
 * 
 * 
 * </pre>
 */
public class TouchCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        long id = ArgParserUtils.toLong(msg.getArgs()[0]);

        boolean touched = client.touch(id);

        if (touched) {
            return new BeanstalkMessage(
                    BeanstalkProtocol.TOUCHED.getCommandName(), null, -1);
        } else {
            return new BeanstalkMessage(
                    BeanstalkProtocol.NOT_FOUND.getCommandName(), null, -1);
        }

    }

}
