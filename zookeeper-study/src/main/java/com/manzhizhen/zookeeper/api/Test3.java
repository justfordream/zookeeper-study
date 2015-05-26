/**
 * 
 */
package com.manzhizhen.zookeeper.api;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * getData获取数据
 * @author manzhizhen
 *
 * 2015年5月25日
 */
public class Test3 implements Watcher{
	private static CountDownLatch coonectLatch = new CountDownLatch(1);
	private static ZooKeeper zookeeper;
	private static Stat stat = new Stat();
	
	public static void main(String[] args) {
		Test3 test = new Test3();
		String path = "/zk-book";
		try {
			zookeeper = new ZooKeeper("192.168.131.101:2181", 5000, test);
			coonectLatch.await();
			zookeeper.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println(zookeeper.getData(path, true, stat));
			
			System.out.println(stat.getCzxid() + "," + stat.getMzxid() + "," + stat.getVersion());
			
			zookeeper.setData(path, "454".getBytes(), -1);
			
			zookeeper.getData(path, true, new MyDataCallback(), null);
			
			zookeeper.setData(path, "4555".getBytes(), -1);
			
			Thread.sleep(50000);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		System.out.println(this + " event:" + event);
		if(KeeperState.SyncConnected == event.getState()) {
			if(EventType.None == event.getType() && null == event.getPath()) {
				coonectLatch.countDown();
			} else if(EventType.NodeChildrenChanged == event.getType()) {
				try {
					System.out.println(new String(zookeeper.getData(event.getPath(), true, stat)));
					System.out.println(stat.getCzxid() + "," + stat.getMzxid() + "," + 
							stat.getVersion());
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	static class MyDataCallback implements AsyncCallback.DataCallback {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			System.out.println("【AsyncCallback.DataCallback】" + rc + ", " + path + ", " + 
				new String(data));
			System.out.println(stat.getCzxid() + ", " + new String(data));
		}
	}
	
}
