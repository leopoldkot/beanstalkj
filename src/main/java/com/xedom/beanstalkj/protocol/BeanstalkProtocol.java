package com.xedom.beanstalkj.protocol;

import java.util.HashMap;
import java.util.Map;

public class BeanstalkProtocol {

    // Error messages:
    public static final MessageStructure INTERNAL_ERROR = new MessageStructure(
            "INTERNAL_ERROR", false);

    public static final MessageStructure OUT_OF_MEMORY = new MessageStructure(
            "OUT_OF_MEMORY", false);

    public static final MessageStructure DRAINING = new MessageStructure(
            "DRAINING", false);

    public static final MessageStructure BAD_FORMAT = new MessageStructure(
            "BAD_FORMAT", false);

    public static final BeanstalkMessage BAD_FORMAT_MSG = new BeanstalkMessage(
            BeanstalkProtocol.BAD_FORMAT.getCommandName(), null, -1);

    public static final MessageStructure UNKNOWN_COMMAND = new MessageStructure(
            "UNKNOWN_COMMAND", false);

    public static final BeanstalkMessage UNKNOWN_COMMAND_MSG = new BeanstalkMessage(
            BeanstalkProtocol.UNKNOWN_COMMAND.getCommandName(), null, -1);

    // Producer Commands

    /**
     * PUT message
     */

    public static final MessageStructure PUT = new MessageStructure("put",
            true, integers("priority", "delay", "timeToRun"));

    public static final MessageStructure INSERTED = new MessageStructure(
            "INSERTED", false, integer("id"));

    public static final MessageStructure BURIED = new MessageStructure(
            "BURIED", false);

    public static final MessageStructure JOB_TOO_BIG = new MessageStructure(
            "JOB_TOO_BIG", false);

    public static final MessageStructure EXPECTED_CRLF = new MessageStructure(
            "EXPECTED_CRLF", false);

    public static final BeanstalkMessage EXPECTED_CRLF_MSG = new BeanstalkMessage(
            BeanstalkProtocol.EXPECTED_CRLF.getCommandName(), null, -1);;

    /**
     * USE message
     */

    public static final MessageStructure USE = new MessageStructure("use",
            false, string("tube"));

    public static final MessageStructure USING = new MessageStructure("USING",
            false, string("tube"));

    // Consumer Commands

    /**
     * RESERVE
     */
    public static final MessageStructure RESERVE = new MessageStructure(
            "reserve", false);

    public static final MessageStructure RESERVED = new MessageStructure(
            "RESERVED", true, integer("id"));

    /**
     * RESERVE WITH TIMEOUT
     */
    public static final MessageStructure RESERVE_WITH_TIMEOUT = new MessageStructure(
            "reserve-with-timeout", false, integer("timeout"));

    public static final MessageStructure TIMED_OUT = new MessageStructure(
            "TIMED_OUT", false);

    public static final MessageStructure DEADLINE_SOON = new MessageStructure(
            "DEADLINE_SOON", false);

    /**
     * DELETE
     */
    public static final MessageStructure DELETE = new MessageStructure(
            "delete", false, integer("id"));

    public static final MessageStructure DELETED = new MessageStructure(
            "DELETED", false);

    public static final MessageStructure NOT_FOUND = new MessageStructure(
            "NOT_FOUND", false);

    /**
     * RELEASE
     */
    public static final MessageStructure RELEASE = new MessageStructure(
            "release", false, integers("id", "priority", "delay"));

    public static final MessageStructure RELEASED = new MessageStructure(
            "RELEASED", false);

    /**
     * BURY
     */
    public static final MessageStructure BURY = new MessageStructure("bury",
            false, integers("id", "priority"));

    /**
     * TOUCH
     */
    public static final MessageStructure TOUCH = new MessageStructure("touch",
            false, integers("id"));

    public static final MessageStructure TOUCHED = new MessageStructure(
            "TOUCHED", false);

    /**
     * WATCH
     */
    public static final MessageStructure WATCH = new MessageStructure("watch",
            false, string("tube"));

    public static final MessageStructure WATCHING = new MessageStructure(
            "WATCHING", false, integer("count"));

    /**
     * IGNORE
     */
    public static final MessageStructure IGNORE = new MessageStructure(
            "ignore", false, string("tube"));

    /**
     * PEEK
     */
    public static final MessageStructure PEEK = new MessageStructure("peek",
            false, integers("id"));

    public static final MessageStructure FOUND = new MessageStructure("FOUND",
            true, integers("id"));

    /**
     * PEEK_READY
     */
    public static final MessageStructure PEEK_READY = new MessageStructure(
            "peek-ready", false);
    /**
     * PEEK_DELAYED
     */
    public static final MessageStructure PEEK_DELAYED = new MessageStructure(
            "peek-delayed", false);
    /**
     * PEEK_BURIED
     */
    public static final MessageStructure PEEK_BURIED = new MessageStructure(
            "peek-buried", false);

    /**
     * KICK
     */
    public static final MessageStructure KICK = new MessageStructure("kick",
            false, integers("bound"));

    public static final MessageStructure KICKED = new MessageStructure(
            "KICKED", false, integers("count"));

    /**
     * STATS_JOB
     */
    public static final MessageStructure STATS_JOB = new MessageStructure(
            "stats-job", false, integer("id"));

    /**
     * STATS_JOB
     */
    public static final MessageStructure STATS_TUBE = new MessageStructure(
            "stats-tube", false, string("tube"));

    /**
     * STATS
     */
    public static final MessageStructure STATS = new MessageStructure("stats",
            false);

    /**
     * LIST_TUBES
     */
    public static final MessageStructure LIST_TUBES = new MessageStructure(
            "list-tubes", false);

    public static final MessageStructure OK = new MessageStructure("OK", true);

    /**
     * LIST_TUBE_USED
     */
    public static final MessageStructure LIST_TUBE_USED = new MessageStructure(
            "list-tube-used", false);

    /**
     * LIST_TUBE_WATCHED
     */
    public static final MessageStructure LIST_TUBES_WATCHED = new MessageStructure(
            "list-tubes-watched", false);

    // cat BeanstalkProtocol.java | grep "public static final MessageStructure"
    // | awk '{print $5 ","}'
    public static final MessageStructure[] ALL_COMMANDS = { INTERNAL_ERROR,
            OUT_OF_MEMORY, DRAINING, BAD_FORMAT, UNKNOWN_COMMAND, PUT,
            INSERTED, BURIED, JOB_TOO_BIG, EXPECTED_CRLF, USE, USING, RESERVE,
            RESERVED, RESERVE_WITH_TIMEOUT, TIMED_OUT, DEADLINE_SOON, DELETE,
            DELETED, NOT_FOUND, RELEASE, RELEASED, BURY, TOUCH, TOUCHED, WATCH,
            WATCHING, IGNORE, PEEK, FOUND, PEEK_READY, PEEK_DELAYED,
            PEEK_BURIED, KICK, KICKED, STATS_JOB, STATS_TUBE, STATS,
            LIST_TUBES, OK, LIST_TUBE_USED, LIST_TUBES_WATCHED };

    public static final Map<String, MessageStructure> COMMAND_BY_NAME = mapByName(ALL_COMMANDS);

    private static Map<String, MessageStructure> mapByName(
            MessageStructure[] allCommands) {

        Map<String, MessageStructure> map = new HashMap<String, MessageStructure>();

        for (MessageStructure s : allCommands) {
            map.put(s.getCommandName(), s);
        }

        return map;
    }

    private static ArgumentStructure string(String name) {
        return new ArgumentStructure(ArgumentType.STRING, name);
    }

    private static ArgumentStructure integer(String name) {
        return new ArgumentStructure(ArgumentType.INTEGER, name);
    }

    private static ArgumentStructure[] integers(String... names) {

        ArgumentStructure[] args = new ArgumentStructure[names.length];

        for (int i = 0; i < names.length; i++) {
            args[i] = integer(names[i]);
        }

        return args;
    }
}
