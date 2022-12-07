package com.puneet;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;


/**
 * @author Sain Technology Solutions
 *
 */
public class ZooKeeperService {
	
	private ZooKeeper zooKeeper;

	private Watcher watcher;
	public ZooKeeperService(final String url, final Watcher processNodeWatcher) throws IOException {
		zooKeeper = new ZooKeeper(url, 3000, processNodeWatcher);
		this.watcher = processNodeWatcher;
	}

	public void removeWatcher(String path) {
		try {
			zooKeeper.removeWatches(path,watcher, Watcher.WatcherType.Any, false);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
	public void addWatch(String path) {
		try {
			zooKeeper.addWatch(path, AddWatchMode.PERSISTENT);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	public byte[] getData(String path) {

		byte[] data = null;
		try {
			data = zooKeeper.getData(path, false, null);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return data;
	}
	public void writeData(String path, String data) {
		try {
			System.out.println("Writing data to node - " + data);
			Stat stat = new Stat();
			zooKeeper.getData(path, false, stat);
			System.out.println("-----data version == " + stat.getVersion());
			zooKeeper.setData(path, data.getBytes(), stat.getVersion());
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public String createNode(final String node, final boolean watch, final boolean ephimeral) {
		String createdNodePath = null;
		try {
			
			final Stat nodeStat =  zooKeeper.exists(node, watch);
			
			if(nodeStat == null) {
				createdNodePath = zooKeeper.create(node, new byte[0], Ids.OPEN_ACL_UNSAFE, (ephimeral ?  CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT));
			} else {
				createdNodePath = node;
			}
			
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return createdNodePath;
	}
	
	public boolean watchNode(final String node, final boolean watch) {
		
		boolean watched = false;
		try {
			final Stat nodeStat =  zooKeeper.exists(node, watch);
			
			if(nodeStat != null) {
				watched = true;
			}
			
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return watched;
	}
	
	public List<String> getChildren(final String node, final boolean watch) {
		
		List<String> childNodes = null;
		
		try {
			childNodes = zooKeeper.getChildren(node, watch);
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return childNodes;
	}
	
}