package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.local.BTJob;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;
import com.xedom.beanstalkj.protocol.error.DeadlineSoonException;

/**
 * <pre>
 * A process that wants to consume jobs from the queue uses &quot;reserve&quot;, &quot;delete&quot;,
 * &quot;release&quot;, and &quot;bury&quot;. The first worker command, &quot;reserve&quot;, looks like this:
 * 
 * reserve\r\n
 * 
 * Alternatively, you can specify a timeout as follows:
 * 
 * reserve-with-timeout &lt;seconds&gt;\r\n
 * 
 * This will return a newly-reserved job. If no job is available to be reserved,
 * beanstalkd will wait to send a response until one becomes available. Once a
 * job is reserved for the client, the client has limited time to run (TTR) the
 * job before the job times out. When the job times out, the server will put the
 * job back into the ready queue. Both the TTR and the actual time left can be
 * found in response to the stats-job command.
 * 
 * A timeout value of 0 will cause the server to immediately return either a
 * response or TIMED_OUT. A positive value of timeout will limit the amount of
 * time the client will block on the reserve request until a job becomes
 * available.
 * 
 * During the TTR of a reserved job, the last second is kept by the server as a
 * safety margin, during which the client will not be made to wait for another
 * job. If the client issues a reserve command during the safety margin, or if
 * the safety margin arrives while the client is waiting on a reserve command,
 * the server will respond with:
 * 
 * DEADLINE_SOON\r\n
 * 
 * This gives the client a chance to delete or release its reserved job before
 * the server automatically releases it.
 * 
 * TIMED_OUT\r\n
 * 
 * If a non-negative timeout was specified and the timeout exceeded before a job
 * became available, the server will respond with TIMED_OUT.
 * 
 * Otherwise, the only other response to this command is a successful
 * reservation in the form of a text line followed by the job body:
 * 
 * RESERVED &lt;id&gt; &lt;bytes&gt;\r\n &lt;data&gt;\r\n
 * 
 * - &lt;id&gt; is the job id -- an integer unique to this job in this instance of
 * beanstalkd.
 * 
 * - &lt;bytes&gt; is an integer indicating the size of the job body, not including
 * the trailing &quot;\r\n&quot;.
 * 
 * - &lt;data&gt; is the job body -- a sequence of bytes of length &lt;bytes&gt; from the
 * previous line. This is a verbatim copy of the bytes that were originally sent
 * to the server in the put command for this job.
 * </pre>
 * 
 */

public class ReserveCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        BTJob reserve;
        try {
            reserve = client.reserve();

            if (reserve != null) {
                String[] args = new String[] { Long.toString(reserve.getId()) };

                BeanstalkMessage beanstalkMessage = new BeanstalkMessage(
                        BeanstalkProtocol.RESERVED.getCommandName(), args,
                        reserve.getData().length);

                beanstalkMessage.setContent(reserve.getData());

                return beanstalkMessage;

            } else {
                BeanstalkMessage beanstalkMessage = new BeanstalkMessage(
                        BeanstalkProtocol.TIMED_OUT.getCommandName());
                return beanstalkMessage;

            }

        } catch (DeadlineSoonException e) {
            BeanstalkMessage beanstalkMessage = new BeanstalkMessage(
                    BeanstalkProtocol.DEADLINE_SOON.getCommandName());
            return beanstalkMessage;
        }

    }
}
