/**
 * 
 */
package com.manzhizhen.zookeeper.api;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 创建一个最基本的Zookeeper会话实例，并服用sessionId和password来构造zookeeper实例
 * 从结果可以看出来，一个会话同时只允许一个客户端去连接，连接成功的客户端会挤掉上次连接成功的客户端，
 * 被挤掉的客户端又会重新尝试连接，就又会挤掉上一个已经连接的客户端，所以最后导致zookeeper和correctKeeper
 * 相互竞争连接的场面
 * @author manzhizhen
 *
 * 2015年5月24日
 */
public class Test1 implements Watcher{
	private CountDownLatch coonectLatch = new CountDownLatch(1);
	
	public CountDownLatch getCountDownLatch() {
		return coonectLatch;
	}
	
	public static void main(String[] args) {
		Test1 test1 = new Test1();
		Test1 test2 = new Test1();
		Test1 test3 = new Test1();
		
		try {
			ZooKeeper zookeeper = new ZooKeeper("192.168.131.101:2181", 5000, test1);
			System.out.println(zookeeper.getState());
			test1.getCountDownLatch().await();
			
			long sessionId = zookeeper.getSessionId();
			byte[] password = zookeeper.getSessionPasswd();
			// 用错误的会话ID和密码来构建ZooKeeper
			ZooKeeper illegalKeeper = new ZooKeeper("192.168.131.101:2181", 5000, test2, 
					1l, "test".getBytes());
			test2.getCountDownLatch().await();
			// 用错误的会话ID和密码来构建ZooKeeper
			ZooKeeper correctKeeper = new ZooKeeper("192.168.131.101:2181", 5000, test3, 
					sessionId, password);
			test3.getCountDownLatch().await();
			System.out.println(illegalKeeper.getState());
			System.out.println(correctKeeper.getState());
			System.out.println(zookeeper.getState());
			
			Thread.sleep(500000);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("over!");
		
	}
	
	@Override
	public void process(WatchedEvent event) {
		System.out.println(this + " watcher event:" + event);
		coonectLatch.countDown();
	}

}
