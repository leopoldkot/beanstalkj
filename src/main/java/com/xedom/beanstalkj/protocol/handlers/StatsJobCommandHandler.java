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
 * The stats-job command gives statistical information about the specified job if
 * it exists. Its form is:
 * 
 * stats-job &lt;id&gt;\r\n
 * 
 *  - &lt;id&gt; is a job id.
 * 
 * The response is one of:
 * 
 *  - &quot;NOT_FOUND\r\n&quot; if the job does not exist.
 * 
 *  - &quot;OK &lt;bytes&gt;\r\n&lt;data&gt;\r\n&quot;
 * 
 *    - &lt;bytes&gt; is the size of the following data section in bytes.
 * 
 *    - &lt;data&gt; is a sequence of bytes of length &lt;bytes&gt; from the previous line. It
 *      is a YAML file with statistical information represented a dictionary.
 * 
 * The stats-job data is a YAML file representing a single dictionary of strings
 * to scalars. It contains these keys:
 * 
 *  - &quot;id&quot; is the job id
 * 
 *  - &quot;tube&quot; is the name of the tube that contains this job
 * 
 *  - &quot;state&quot; is &quot;ready&quot; or &quot;delayed&quot; or &quot;reserved&quot; or &quot;buried&quot;
 * 
 *  - &quot;pri&quot; is the priority value set by the put, release, or bury commands.
 * 
 *  - &quot;age&quot; is the time in seconds since the put command that created this job.
 * 
 *  - &quot;time-left&quot; is the number of seconds left until the server puts this job
 *    into the ready queue. This number is only meaningful if the job is
 *    reserved or delayed. If the job is reserved and this amount of time
 *    elapses before its state changes, it is considered to have timed out.
 * 
 *  - &quot;timeouts&quot; is the number of times this job has timed out during a
 *    reservation.
 * 
 *  - &quot;releases&quot; is the number of times a client has released this job from a
 *    reservation.
 * 
 *  - &quot;buries&quot; is the number of times this job has been buried.
 * 
 *  - &quot;kicks&quot; is the number of times this job has been kicked.
 * 
 * 
 * </pre>
 */
public class StatsJobCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        long id = ArgParserUtils.toLong(msg.getArgs()[0]);

        Map<String, String> statsJob = client.statsJob(id);

        if (statsJob != null) {
            Yaml yaml = new Yaml();
            String dump = yaml.dump(statsJob);

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
