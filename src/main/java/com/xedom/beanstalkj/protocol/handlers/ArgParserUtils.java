package com.xedom.beanstalkj.protocol.handlers;

import com.xedom.beanstalkj.protocol.error.BadFormatException;

public class ArgParserUtils {

    public static int toInt(String string) throws BadFormatException {

        int parseInt = 0;
        try {
            parseInt = Integer.parseInt(string);
        } catch (NumberFormatException nfe) {
            throw new BadFormatException("Can't parse an integer value: "
                    + string, string);
        }

        return parseInt;
    }

    public static long toLong(String string) throws BadFormatException {
        long val = 0;
        try {
            val = Long.parseLong(string);
        } catch (NumberFormatException nfe) {
            throw new BadFormatException("Can't parse a long value: " + string,
                    string);
        }

        return val;
    }

}
