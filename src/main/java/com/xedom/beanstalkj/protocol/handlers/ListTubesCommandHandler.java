package com.xedom.beanstalkj.protocol.handlers;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.yaml.snakeyaml.Yaml;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The list-tubes command returns a list of all existing tubes. Its form is:
 * 
 * list-tubes\r\n
 * 
 * The response is:
 * 
 * OK &lt;bytes&gt;\r\n
 * &lt;data&gt;\r\n
 * 
 *  - &lt;bytes&gt; is the size of the following data section in bytes.
 * 
 *  - &lt;data&gt; is a sequence of bytes of length &lt;bytes&gt; from the previous line. It
 *    is a YAML file containing all tube names as a list of strings.
 * </pre>
 */
public class ListTubesCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        List<String> listTubes = client.listTubes();

        Yaml yaml = new Yaml();
        String dump = yaml.dump(listTubes);

        byte[] bytes;
        try {
            bytes = dump.getBytes("ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
        BeanstalkMessage beanstalkMessage = new BeanstalkMessage(
                BeanstalkProtocol.OK.getCommandName(), null, bytes.length);

        beanstalkMessage.setContent(bytes);

        return beanstalkMessage;
    }

}
