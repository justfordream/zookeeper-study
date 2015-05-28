/**
 * 
 */
package com.manzhizhen.zookeeper.api;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * @author 易振强
 *
 * 2015年5月26日
 */
public class MyTest implements Watcher{

	private static ZooKeeper zookeeper = null;
	private static MyTest test = null;
	private CountDownLatch latch = new CountDownLatch(1);
	private Stat stat = new Stat();
	
	public CountDownLatch getLatch() {
		return latch;
	}
	
	public Stat getStat() {
		return stat;
	}
	
	public static void main(String[] args) {
		test = new MyTest();
		try {
			zookeeper = new ZooKeeper("10.0.53.51:2181", 5000, test);
			test.getLatch().await();
			System.out.println("连接成功... ...");
			
			zookeeper.getData("/zk-test", test, test.getStat());
			
			Thread.sleep(500000);
			
			System.out.println("over... ...");
			
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
		System.out.println("监听器接收到event：" + event);
		if(KeeperState.SyncConnected == event.getState()) {
			latch.countDown();
		}
		
		if(EventType.NodeDataChanged == event.getType()) {
			// 注意，此时的stat是该节点修改前的stat
			System.out.println("接收到节点数据改变,数据改变前的stat:" + pringStat(test.getStat()));
			// 监听一次后，该监听器会立即失效，所以每次都得重新注册一次
			try {
				zookeeper.getData("/zk-test", test, test.getStat());
				System.out.println("接收到节点数据改变,数据改变后的stat:" + pringStat(test.getStat()));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else if(EventType.NodeChildrenChanged == event.getType()) {
			System.out.println("接收到子节点变化");
			try {
				System.out.println(zookeeper.getChildren("/zk-test", true));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static String pringStat(Stat stat) {
		return ReflectionToStringBuilder.toString(stat, ToStringStyle.NO_CLASS_NAME_STYLE);
	}

}
