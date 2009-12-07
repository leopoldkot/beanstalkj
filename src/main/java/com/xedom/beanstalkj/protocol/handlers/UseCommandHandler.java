package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The &quot;use&quot; command is for producers. Subsequent put commands will put jobs into
 * the tube specified by this command. If no use command has been issued, jobs
 * will be put into the tube named &quot;default&quot;.
 * 
 * use &lt;tube&gt;\r\n
 * 
 *  - &lt;tube&gt; is a name at most 200 bytes. It specifies the tube to use. If the
 *    tube does not exist, it will be created.
 * 
 * The only reply is:
 * 
 * USING &lt;tube&gt;\r\n
 * 
 *  - &lt;tube&gt; is the name of the tube now being used.
 * 
 * </pre>
 */
public class UseCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        String tube = msg.getArgs()[0];

        client.useTube(tube);

        String[] args = new String[] { tube };

        return new BeanstalkMessage(BeanstalkProtocol.USING.getCommandName(),
                args, -1);
    }

}
