package com.qc.itaojin;

import com.qc.itaojin.util.JsonUtil;
import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by fuqinqin on 2018/7/4.
 */
public class ConnectTest {

    private static final String ZK_SERVERS = "itaojin105:2181,itaojin106:2181,itaojin107:2181";
    private static final String COMPANY = "/itaojin";
    private static final String DEPARTMENT = "/qc";
    private static final String PROJECT = "/canal-client";
    private static final String RUNNING = "/running";

    ZooKeeper zk;

    private int num = 10;
    /*
     * 线程计数器
     * 	将线程数量初始化
     * 	每执行完成一条线程，调用countDown()使计数器减1
     * 	主线程调用方法await()使其等待，当计数器为0时才被执行
     */
    private CountDownLatch latch = new CountDownLatch(num);

    @Before
    public void init() throws IOException {
        zk = new ZooKeeper(ZK_SERVERS, 5000, null);
    }

    @Test
    public void initFolder() throws Exception {
        String now = "init time : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        if (zk.exists(COMPANY, false) == null) {
            System.out.println(zk.create(COMPANY, now.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        }

        if (zk.exists(COMPANY + DEPARTMENT, false) == null) {
            System.out.println(zk.create(COMPANY + DEPARTMENT, now.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        }

        if (zk.exists(COMPANY + DEPARTMENT + PROJECT, false) == null) {
            System.out.println(zk.create(COMPANY + DEPARTMENT + PROJECT, now.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        }
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/", false);
        for (String child : children) {
            System.out.println(child);
        }
    }

    @Test
    public void lsAll() throws Exception {
        showAll("/");
    }

    private void showAll(String path) throws Exception {
        System.out.println(path);
        List<String> children = zk.getChildren(path, false);
        if (children == null || children.isEmpty()) {
            return;
        }
        for (String child : children) {
            if (path.equals("/")) {
                showAll(path + child);
            } else {
                showAll(path + "/" + child);
            }
        }
    }

    @Test
    public void createEphemeralMode() throws Exception {
        String path = StringUtils.contact(COMPANY, DEPARTMENT, PROJECT, RUNNING);
        TestEntity entity = new TestEntity();
        entity.setActive(true);
        entity.setThreadId(101L);
        entity.setThreadName("thread-101");
        String p = zk.create(path, JsonUtil.toJson(entity).getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("result: " + p);

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 多线程创建临时节点
     */
    @Test
    public void multiThreadCreateEphemeralMode() throws Exception {
        for (int i = 0; i < num; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    String path = StringUtils.contact(COMPANY, DEPARTMENT, PROJECT, RUNNING);
                    TestEntity entity = new TestEntity();
                    entity.setActive(true);
                    entity.setThreadId(Thread.currentThread().getId());
                    entity.setThreadName(Thread.currentThread().getName());
                    String p = null;
                    try {
                        p = zk.create(path, JsonUtil.toJson(entity).getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                        System.out.println("result: " + p);
                        System.out.println(Thread.currentThread().getId() + " " + Thread.currentThread().getName());

                        while (true) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }

                    latch.countDown();
                }
            }).start();
        }

        try {
            latch.await(); // 主线程等待
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 监听某个目录
    @Test
    public void watchNode() throws Exception {
        zk.getData(StringUtils.contact(COMPANY, DEPARTMENT, PROJECT, RUNNING), new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("in ============== "+Thread.currentThread().getId());
                Event.EventType type = watchedEvent.getType();
                System.out.println(type.toString());
                if (type == Event.EventType.NodeDeleted) {
                    System.out.println("Deleted! " + watchedEvent.getPath() + " === " + watchedEvent.toString());
                    Thread.currentThread().interrupt();
                } else if(type == Event.EventType.NodeCreated){
                    System.out.println("Created! " + watchedEvent.getPath() + " === " + watchedEvent.toString());
                }
            }
        }, null);

        System.out.println("out ============== "+Thread.currentThread().getId());

        while (true) {
            try {
                System.out.println("-- out --");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Data
    public static class TestEntity {
        private boolean active;
        private long threadId;
        private String threadName;
    }

}
