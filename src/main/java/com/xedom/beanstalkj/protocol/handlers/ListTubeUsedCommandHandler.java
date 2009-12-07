package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The list-tube-used command returns the tube currently being used by the
 * client. Its form is:
 * 
 * list-tube-used\r\n
 * 
 * The response is:
 * 
 * USING &lt;tube&gt;\r\n
 * 
 *  - &lt;tube&gt; is the name of the tube being used.
 * </pre>
 */
public class ListTubeUsedCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        String tubeUsed = client.listTubeUsed();

        String[] args = new String[] { tubeUsed };
        return new BeanstalkMessage(BeanstalkProtocol.USING.getCommandName(),
                args, -1);

    }

}
