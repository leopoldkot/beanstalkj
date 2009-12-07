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
 * The stats command gives statistical information about the system as a whole.
 * Its form is:
 * 
 * stats\r\n
 * 
 * The server will respond:
 * 
 * OK &lt;bytes&gt;\r\n
 * &lt;data&gt;\r\n
 * 
 *  - &lt;bytes&gt; is the size of the following data section in bytes.
 * 
 *  - &lt;data&gt; is a sequence of bytes of length &lt;bytes&gt; from the previous line. It
 *    is a YAML file with statistical information represented a dictionary.
 * 
 * The stats data for the system is a YAML file representing a single dictionary
 * of strings to scalars. It contains these keys:
 * 
 *  - &quot;current-jobs-urgent&quot; is the number of ready jobs with priority &lt; 1024.
 * 
 *  - &quot;current-jobs-ready&quot; is the number of jobs in the ready queue.
 * 
 *  - &quot;current-jobs-reserved&quot; is the number of jobs reserved by all clients.
 * 
 *  - &quot;current-jobs-delayed&quot; is the number of delayed jobs.
 * 
 *  - &quot;current-jobs-buried&quot; is the number of buried jobs.
 * 
 *  - &quot;cmd-put&quot; is the cumulative number of put commands.
 * 
 *  - &quot;cmd-peek&quot; is the cumulative number of peek commands.
 * 
 *  - &quot;cmd-peek-ready&quot; is the cumulative number of peek-ready commands.
 * 
 *  - &quot;cmd-peek-delayed&quot; is the cumulative number of peek-delayed commands.
 * 
 *  - &quot;cmd-peek-buried&quot; is the cumulative number of peek-buried commands.
 * 
 *  - &quot;cmd-reserve&quot; is the cumulative number of reserve commands.
 * 
 *  - &quot;cmd-use&quot; is the cumulative number of use commands.
 * 
 *  - &quot;cmd-watch&quot; is the cumulative number of watch commands.
 * 
 *  - &quot;cmd-ignore&quot; is the cumulative number of ignore commands.
 * 
 *  - &quot;cmd-delete&quot; is the cumulative number of delete commands.
 * 
 *  - &quot;cmd-release&quot; is the cumulative number of release commands.
 * 
 *  - &quot;cmd-bury&quot; is the cumulative number of bury commands.
 * 
 *  - &quot;cmd-kick&quot; is the cumulative number of kick commands.
 * 
 *  - &quot;cmd-stats&quot; is the cumulative number of stats commands.
 * 
 *  - &quot;cmd-stats-job&quot; is the cumulative number of stats-job commands.
 * 
 *  - &quot;cmd-stats-tube&quot; is the cumulative number of stats-tube commands.
 * 
 *  - &quot;cmd-list-tubes&quot; is the cumulative number of list-tubes commands.
 * 
 *  - &quot;cmd-list-tube-used&quot; is the cumulative number of list-tube-used commands.
 * 
 *  - &quot;cmd-list-tubes-watched&quot; is the cumulative number of list-tubes-watched
 *    commands.
 * 
 *  - &quot;job-timeouts&quot; is the cumulative count of times a job has timed out.
 * 
 *  - &quot;total-jobs&quot; is the cumulative count of jobs created.
 * 
 *  - &quot;max-job-size&quot; is the maximum number of bytes in a job.
 * 
 *  - &quot;current-tubes&quot; is the number of currently-existing tubes.
 * 
 *  - &quot;current-connections&quot; is the number of currently open connections.
 * 
 *  - &quot;current-producers&quot; is the number of open connections that have each
 *    issued at least one put command.
 * 
 *  - &quot;current-workers&quot; is the number of open connections that have each issued
 *    at least one reserve command.
 * 
 *  - &quot;current-waiting&quot; is the number of open connections that have issued a
 *    reserve command but not yet received a response.
 * 
 *  - &quot;total-connections&quot; is the cumulative count of connections.
 * 
 *  - &quot;pid&quot; is the process id of the server.
 * 
 *  - &quot;version&quot; is the version string of the server.
 * 
 *  - &quot;rusage-utime&quot; is the accumulated user CPU time of this process in seconds
 *    and microseconds.
 * 
 *  - &quot;rusage-stime&quot; is the accumulated system CPU time of this process in
 *    seconds and microseconds.
 * 
 *  - &quot;uptime&quot; is the number of seconds since this server started running.
 * 
 *  - &quot;binlog-oldest-index&quot; is the index of the oldest binlog file needed to
 *    store the current jobs
 * 
 *  - &quot;binlog-current-index&quot; is the index of the current binlog file being
 *    written to. If binlog is not active this value will be 0
 * 
 *  - &quot;binlog-max-size&quot; is the maximum size in bytes a binlog file is allowed
 *    to get before a new binlog file is opened
 * 
 * </pre>
 */
public class StatsCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        Map<String, String> stats = client.stats();

        Yaml yaml = new Yaml();
        String dump = yaml.dump(stats);

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
