package com.xedom.beanstalkj.protocol;

import java.util.Arrays;

import com.xedom.beanstalkj.protocol.error.BadFormatException;
import com.xedom.beanstalkj.protocol.error.UnknownCommandException;

public class MessageParser {

    BeanstalkMessage parseHeader(String commandLine) throws BadFormatException,
            UnknownCommandException {

        String[] tokens = commandLine.split(" ");

        if (tokens == null || tokens.length == 0) {
            throw new BadFormatException("Request is empty", commandLine);
        }

        String commandName = tokens[0].trim();
        MessageStructure structure = lookupStructure(commandName);
        if (structure == null) {
            throw new UnknownCommandException("Command is not found: "
                    + commandName, commandLine);
        }

        return parseMessage(commandLine, structure, tokens);
    }

    private BeanstalkMessage parseMessage(String commandLine,
            MessageStructure structure, String[] tokens)
            throws BadFormatException {

        // check size: commandName + args + [body length]
        int expectedCommandNameSize = 1;
        int expectedArgsSize = structure.getArgs().length;
        int expectedContentLengthSize = structure.hasBody() ? 1 : 0;
        int expectedAllSize = expectedCommandNameSize + expectedArgsSize
                + expectedContentLengthSize;

        if (expectedAllSize != tokens.length) {
            throw new BadFormatException("Arguments number is wrong: "
                    + commandLine, commandLine);
        }

        String[] args = Arrays.copyOfRange(tokens, expectedCommandNameSize,
                expectedArgsSize + 1);

        for (int i = 0; i < expectedArgsSize; i++) {
            ArgumentStructure argumentStructure = structure.getArgs()[i];

            String arg = args[i].trim();
            if (argumentStructure.getType().equals(ArgumentType.INTEGER)) {
                try {
                    Integer value = Integer.valueOf(arg);
                    if (value < 0) {
                        throw new BadFormatException("Argument "
                                + argumentStructure.getName()
                                + " should be positive integer: " + arg,
                                commandLine);
                    }

                } catch (NumberFormatException nfe) {
                    throw new BadFormatException("Argument "
                            + argumentStructure.getName()
                            + " should be integer: " + arg, commandLine);
                }
            } else if (argumentStructure.getType().equals(ArgumentType.STRING)) {
                if (arg.length() == 0) {
                    throw new BadFormatException("Argument "
                            + argumentStructure.getName()
                            + " should be non empty string", commandLine);
                }

                if (!arg.matches("^[a-zA-Z0-9+/.;$()][a-zA-Z0-9-+/.;$()]*$")) {
                    throw new BadFormatException("Argument "
                            + argumentStructure.getName()
                            + " should match [a-zA-Z0-9-+/.;$()]+", commandLine);
                }

                // TODO: add ASCII and regexp check

            }

        }

        int contentLength = -1;
        if (structure.hasBody()) {
            String contentLengthToken = tokens[expectedAllSize - 1];

            try {
                contentLength = Integer.valueOf(contentLengthToken);
                if (contentLength < 0) {
                    throw new BadFormatException(
                            "Content length should be positive integer: "
                                    + contentLengthToken, commandLine);
                }

            } catch (NumberFormatException nfe) {
                throw new BadFormatException(
                        "Content length should be integer: "
                                + contentLengthToken, commandLine);
            }

        }

        BeanstalkMessage msg = new BeanstalkMessage(tokens[0].trim(), args,
                contentLength);

        return msg;

    }

    private MessageStructure lookupStructure(String commandName) {

        MessageStructure structure = BeanstalkProtocol.COMMAND_BY_NAME.get(commandName);

        return structure;
    }

}
