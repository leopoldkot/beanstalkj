package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The &quot;watch&quot; command adds the named tube to the watch list for the current
 * connection. A reserve command will take a job from any of the tubes in the
 * watch list. For each new connection, the watch list initially consists of one
 * tube, named &quot;default&quot;.
 * 
 * watch &lt;tube&gt;\r\n
 * 
 *  - &lt;tube&gt; is a name at most 200 bytes. It specifies a tube to add to the watch
 *    list. If the tube doesn't exist, it will be created.
 * 
 * The reply is:
 * 
 * WATCHING &lt;count&gt;\r\n
 * 
 *  - &lt;count&gt; is the integer number of tubes currently in the watch list.
 * 
 * </pre>
 */
public class WatchCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        String tube = msg.getArgs()[0];

        int watchCount = client.watch(tube);

        String[] args = new String[] { Integer.toString(watchCount) };

        return new BeanstalkMessage(
                BeanstalkProtocol.WATCHING.getCommandName(), args, -1);
    }

}
