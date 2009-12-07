package com.xedom.beanstalkj.protocol.handlers;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * <pre>
 * The stats-tube command gives statistical information about the specified tube
 * if it exists. Its form is:
 * 
 * stats-tube &lt;tube&gt;\r\n
 * 
 *  - &lt;tube&gt; is a name at most 200 bytes. Stats will be returned for this tube.
 * 
 * The response is one of:
 * 
 *  - &quot;NOT_FOUND\r\n&quot; if the tube does not exist.
 * 
 *  - &quot;OK &lt;bytes&gt;\r\n&lt;data&gt;\r\n&quot;
 * 
 *    - &lt;bytes&gt; is the size of the following data section in bytes.
 * 
 *    - &lt;data&gt; is a sequence of bytes of length &lt;bytes&gt; from the previous line. It
 *      is a YAML file with statistical information represented a dictionary.
 * 
 * The stats-tube data is a YAML file representing a single dictionary of strings
 * to scalars. It contains these keys:
 * 
 *  - &quot;name&quot; is the tube's name.
 * 
 *  - &quot;current-jobs-urgent&quot; is the number of ready jobs with priority &lt; 1024 in
 *    this tube.
 * 
 *  - &quot;current-jobs-ready&quot; is the number of jobs in the ready queue in this tube.
 * 
 *  - &quot;current-jobs-reserved&quot; is the number of jobs reserved by all clients in
 *    this tube.
 * 
 *  - &quot;current-jobs-delayed&quot; is the number of delayed jobs in this tube.
 * 
 *  - &quot;current-jobs-buried&quot; is the number of buried jobs in this tube.
 * 
 *  - &quot;total-jobs&quot; is the cumulative count of jobs created in this tube.
 * 
 *  - &quot;current-waiting&quot; is the number of open connections that have issued a
 *    reserve command while watching this tube but not yet received a response.
 * 
 * </pre>
 */
public class StatsTubeCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        String tube = msg.getArgs()[0];

        Map<String, String> statsTube = client.statsTube(tube);

        if (statsTube != null) {
            Yaml yaml = new Yaml();
            String dump = yaml.dump(statsTube);

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

        } else {
            return new BeanstalkMessage(
                    BeanstalkProtocol.NOT_FOUND.getCommandName(), null, -1);
        }

    }

}
