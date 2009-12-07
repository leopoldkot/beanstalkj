package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The &quot;ignore&quot; command is for consumers. It removes the named tube from the
 * watch list for the current connection.
 * 
 * ignore &lt;tube&gt;\r\n
 * 
 * The reply is one of:
 * 
 *  - &quot;WATCHING &lt;count&gt;\r\n&quot; to indicate success.
 * 
 *    - &lt;count&gt; is the integer number of tubes currently in the watch list.
 * 
 *  - &quot;NOT_IGNORED\r\n&quot; if the client attempts to ignore the only tube in its
 *    watch list.
 * 
 * </pre>
 */
public class IgnoreCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        String tube = msg.getArgs()[0];

        int ignore = client.ignore(tube);
        // TODO: handle NOT_IGNORED

        String[] args = new String[] { Integer.toString(ignore) };
        return new BeanstalkMessage(
                BeanstalkProtocol.WATCHING.getCommandName(), args, -1);
    }

}
