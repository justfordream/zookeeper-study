/**
 * 
 */
package com.manzhizhen.zookeeper.api;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 创建和删除节点
 * @author manzhizhen
 *
 * 2015年5月24日
 */
public class Test2 implements Watcher{
	private CountDownLatch coonectLatch = new CountDownLatch(1);
	private static ZooKeeper zookeeper;
	
	public CountDownLatch getCountDownLatch() {
		return coonectLatch;
	}
	
	public static void main(String[] args) {
		Test2 test = new Test2();
		try {
			zookeeper = new ZooKeeper("192.168.131.101:2181", 5000, test);
			test.getCountDownLatch().await();
			zookeeper.register(test);
			// 同步创建一个持久节点
			String path = zookeeper.create("/zk-test", "111".getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("success create znode:" + path);
			// 同步创建一个临时节点
			String path1 = zookeeper.create("/zk-test1", "222".getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println("success create znode:" + path1);
			// 同步创建一个临时顺序节点
			String path2 = zookeeper.create("/zk-test1", "333".getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("success create znode:" + path2);
			// 异步创建节点
			zookeeper.create("/zk-test2", "444".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, 
					new MyStringCallback(), "love me");
			zookeeper.create("/zk-test2", "555".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, 
					new MyStringCallback(), "love me");
			
			List<String> childrenList = zookeeper.getChildren("/", test);
			System.out.println(childrenList);
			zookeeper.getChildren(path, test, new MyChildren2Classback(), null);
			
			
			// 持久化节点下面才能新建子节点
			// 创建新的子节点，将通知观察器
			zookeeper.create(path + "tt1", "666".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, 
					new MyStringCallback(), "love me");
			zookeeper.create(path + "/tt2", "777".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, 
					new MyStringCallback(), "love me");
			
//			zookeeper.delete("/zk-test2", );
			
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
					System.out.println("ReGet Child:" + zookeeper.getChildren(event.getPath(), true) );
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	static class MyStringCallback implements AsyncCallback.StringCallback {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			System.out.println(String.format("create path result:[%s, %s, %s, real path name:%s]", 
					new Object[]{rc, path, ctx, name}));
		}
		
	}
	
	static class MyChildren2Classback implements Children2Callback {
		private Stat stat;
		
		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children, Stat stat) {
			System.out.println(this + "stat:" + stat);
			this.stat = stat;
		}
		
		public Stat getStat() {
			return stat;
		}
	}
}
