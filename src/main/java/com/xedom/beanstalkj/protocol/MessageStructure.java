package com.xedom.beanstalkj.protocol;

public class MessageStructure {

    private String commandName;

    private boolean hasBody;

    private ArgumentStructure[] args;

    public MessageStructure(String commandName, boolean hasBody,
            ArgumentStructure... args) {
        super();
        this.commandName = commandName;
        this.hasBody = hasBody;
        this.args = args;
    }

    public String getCommandName() {
        return commandName;
    }

    public boolean hasBody() {
        return hasBody;
    }

    public ArgumentStructure[] getArgs() {
        return args;
    }

}

class ArgumentStructure {

    private ArgumentType type;

    private String name;

    public ArgumentStructure(ArgumentType type, String name) {
        super();
        this.type = type;
        this.name = name;
    }

    public ArgumentType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

}

enum ArgumentType {
    INTEGER, STRING
}