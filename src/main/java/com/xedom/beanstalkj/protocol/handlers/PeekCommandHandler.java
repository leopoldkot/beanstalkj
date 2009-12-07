package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.local.BTJob;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The peek commands let the client inspect a job in the system. There are four
 * variations. All but the first operate only on the currently used tube.
 * 
 *  - &quot;peek &lt;id&gt;\r\n&quot; - return job &lt;id&gt;.
 *  
 *  There are two possible responses, either a single line:
 * 
 *  - &quot;NOT_FOUND\r\n&quot; if the requested job doesn't exist or there are no jobs in
 *    the requested state.
 * 
 * Or a line followed by a chunk of data, if the command was successful:
 * 
 * FOUND &lt;id&gt; &lt;bytes&gt;\r\n
 * &lt;data&gt;\r\n
 * 
 *  - &lt;id&gt; is the job id.
 * 
 *  - &lt;bytes&gt; is an integer indicating the size of the job body, not including
 *    the trailing &quot;\r\n&quot;.
 * 
 *  - &lt;data&gt; is the job body -- a sequence of bytes of length &lt;bytes&gt; from the
 *    previous line.
 * 
 * 
 * </pre>
 */
public class PeekCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        long jobId = ArgParserUtils.toLong(msg.getArgs()[0]);

        BTJob job = client.peek(jobId);

        if (job != null) {
            String[] args = new String[] { Long.toString(jobId) };

            BeanstalkMessage beanstalkMessage = new BeanstalkMessage(
                    BeanstalkProtocol.FOUND.getCommandName(), args,
                    job.getData().length);

            beanstalkMessage.setContent(job.getData());

            return beanstalkMessage;
        } else {
            return new BeanstalkMessage(
                    BeanstalkProtocol.NOT_FOUND.getCommandName(), null, -1);
        }
    }

}
