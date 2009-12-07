package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

public interface CommandHandler {
    BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException;

}
