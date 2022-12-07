/**
 *
 */
package com.puneet;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;


/**
 * @author Sain Technology Solutions
 *
 */
public class ProcessNode implements Runnable{

    private static final Logger LOG = Logger.getLogger(ProcessNode.class);

    private static final String LEADER_ELECTION_ROOT_NODE = "/election";
    private static final String PROCESS_NODE_PREFIX = "/p_";

    private final int id;
    private ZooKeeperService zooKeeperService;

    private String processNodePath;
    private String watchedNodePath;

    public long counter = 0;
    private final String zkURL;

    public ProcessNode(final int id, final String zkURL) {
        this.id = id;
        this.zkURL = zkURL;
    }

    public void initialize() throws IOException {
        zooKeeperService = new ZooKeeperService(zkURL, new ProcessNodeWatcher(this));
    }

    private void attemptForLeaderPosition() {

        final List<String> childNodePaths = zooKeeperService.getChildren(LEADER_ELECTION_ROOT_NODE, true);

        Collections.sort(childNodePaths);

        int index = childNodePaths.indexOf(processNodePath.substring(processNodePath.lastIndexOf('/') + 1));
        if(index == 0) {
            if(LOG.isInfoEnabled()) {
                LOG.info("[Process: " + id + "] I am the new leader!");
            }

            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!Thread.interrupted()) {
                        System.out.println("---Writing - " + ProcessNode.this.counter);
                        zooKeeperService.writeData(LEADER_ELECTION_ROOT_NODE, String.valueOf(ProcessNode.this.counter));
                        ProcessNode.this.counter++;
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            return;
                        }
                    }
                }
            });
            t.setDaemon(true);
            t.start();
            zooKeeperService.removeWatcher(LEADER_ELECTION_ROOT_NODE);
        } else {
            final String watchedNodeShortPath = childNodePaths.get(index - 1);

            watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeShortPath;

            if(LOG.isInfoEnabled()) {
                LOG.info("[Process: " + id + "] - Setting watch on node with path: " + watchedNodePath);
            }
            zooKeeperService.watchNode(watchedNodePath, true);
            zooKeeperService.addWatch(LEADER_ELECTION_ROOT_NODE);
        }
    }

    @Override
    public void run() {

        if(LOG.isInfoEnabled()) {
            LOG.info("Process with id: " + id + " has started!");
        }

        final String rootNodePath = zooKeeperService.createNode(LEADER_ELECTION_ROOT_NODE, false, false);
        if(rootNodePath == null) {
            throw new IllegalStateException("Unable to create/access leader election root node with path: " + LEADER_ELECTION_ROOT_NODE);
        }

        processNodePath = zooKeeperService.createNode(rootNodePath + PROCESS_NODE_PREFIX, false, true);
        if(processNodePath == null) {
            throw new IllegalStateException("Unable to create/access process node with path: " + LEADER_ELECTION_ROOT_NODE);
        }

        if(LOG.isDebugEnabled()) {
            LOG.debug("[Process: " + id + "] Process node created with path: " + processNodePath);
        }

        attemptForLeaderPosition();
    }

    public class ProcessNodeWatcher implements Watcher{

        ProcessNode processNode;
        public ProcessNodeWatcher(ProcessNode processNode) {
            this.processNode = processNode;
        }

        @Override
        public void process(WatchedEvent event) {
            //if(LOG.isDebugEnabled()) {
                LOG.info("[Process: " + id + "] Event received: " + event);
           // }

            final EventType eventType = event.getType();
            if(EventType.NodeDataChanged.equals(eventType)){
                byte[] data = zooKeeperService.getData(event.getPath());
                String str = new String(data);
                System.out.println("----Received -- " + str);
                processNode.counter = Long.valueOf(str);
            }else if(EventType.NodeDeleted.equals(eventType)) {
                if(event.getPath().equalsIgnoreCase(watchedNodePath)) {
                    attemptForLeaderPosition();
                }
            }
        }

    }

}