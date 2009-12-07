package com.xedom.beanstalkj.local;

import java.util.HashSet;
import java.util.Set;

public class BTTubeMux {

    private Set<BTTube> tubes = new HashSet<BTTube>();

    public void addTube(BTTube tube) {
        tubes.add(tube);
    }

    public void removeTube(BTTube tube) {
        tubes.remove(tube);
    }

}
