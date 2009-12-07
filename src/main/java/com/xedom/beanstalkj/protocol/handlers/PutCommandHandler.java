package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.local.BTClient;
import com.xedom.beanstalkj.protocol.BeanstalkMessage;
import com.xedom.beanstalkj.protocol.BeanstalkProtocol;
import com.xedom.beanstalkj.protocol.error.BadFormatException;

/**
 * 
 * The "put" command is for any process that wants to insert a job into the
 * queue. It comprises a command line followed by the job body:
 * 
 * put <pri> <delay> <ttr> <bytes>\r\n <data>\r\n
 * 
 * It inserts a job into the client's currently used tube (see the "use" command
 * below).
 * 
 * - <pri> is an integer < 2**32. Jobs with smaller priority values will be
 * scheduled before jobs with larger priorities. The most urgent priority is 0;
 * the least urgent priority is 4294967295.
 * 
 * - <delay> is an integer number of seconds to wait before putting the job in
 * the ready queue. The job will be in the "delayed" state during this time.
 * 
 * - <ttr> -- time to run -- is an integer number of seconds to allow a worker
 * to run this job. This time is counted from the moment a worker reserves this
 * job. If the worker does not delete, release, or bury the job within <ttr>
 * seconds, the job will time out and the server will release the job. The
 * minimum ttr is 1. If the client sends 0, the server will silently increase
 * the ttr to 1.
 * 
 * - <bytes> is an integer indicating the size of the job body, not including
 * the trailing "\r\n". This value must be less than max-job-size (default:
 * 2**16).
 * 
 * - <data> is the job body -- a sequence of bytes of length <bytes> from the
 * previous line.
 * 
 * After sending the command line and body, the client waits for a reply, which
 * may be:
 * 
 * - "INSERTED <id>\r\n" to indicate success.
 * 
 * - <id> is the integer id of the new job
 * 
 * - "BURIED <id>\r\n" if the server ran out of memory trying to grow the
 * priority queue data structure.
 * 
 * - <id> is the integer id of the new job
 * 
 * - "EXPECTED_CRLF\r\n" The job body must be followed by a CR-LF pair, that is,
 * "\r\n". These two bytes are not counted in the job size given by the client
 * in the put command line.
 * 
 * - "JOB_TOO_BIG\r\n" The client has requested to put a job with a body larger
 * than max-job-size bytes.
 * 
 * 
 */

public class PutCommandHandler implements CommandHandler {

    @Override
    public BeanstalkMessage handleCommand(BTClient client, BeanstalkMessage msg)
            throws BadFormatException {

        long priority = ArgParserUtils.toLong(msg.getArgs()[0]);
        int delaySeconds = ArgParserUtils.toInt(msg.getArgs()[1]);
        int timeToRun = ArgParserUtils.toInt(msg.getArgs()[2]);
        byte[] data = msg.getContent();
        long jobId = client.put(priority, delaySeconds, timeToRun, data);

        String[] args = new String[] { Long.toString(jobId) };

        return new BeanstalkMessage(
                BeanstalkProtocol.INSERTED.getCommandName(), args, -1);
    }
}
