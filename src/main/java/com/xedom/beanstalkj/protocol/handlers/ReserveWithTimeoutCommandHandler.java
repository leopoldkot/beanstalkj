package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.local.BTJob;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;
import com.xedom.beanstalkj.protocol.error.DeadlineSoonException;

public class ReserveWithTimeoutCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        int timeout = ArgParserUtils.toInt(msg.getArgs()[0]);
        BTJob reserve;
        try {
            reserve = client.reserve(timeout);

            if (reserve == null) {
                return new BeanstalkMessage(
                        BeanstalkProtocol.TIMED_OUT.getCommandName(), null, -1);
            } else {
                String[] args = new String[] { Long.toString(reserve.getId()) };

                BeanstalkMessage beanstalkMessage = new BeanstalkMessage(
                        BeanstalkProtocol.RESERVED.getCommandName(), args,
                        reserve.getData().length);

                beanstalkMessage.setContent(reserve.getData());

                return beanstalkMessage;
            }

        } catch (DeadlineSoonException e) {
            BeanstalkMessage beanstalkMessage = new BeanstalkMessage(
                    BeanstalkProtocol.DEADLINE_SOON.getCommandName());
            return beanstalkMessage;
        }

    }
}
