package com.qc.itaojin.common;

import com.qc.itaojin.service.ZookeeperServiceImpl;
import com.qc.itaojin.util.InetAddressUtil;
import com.qc.itaojin.util.JsonUtil;
import com.qc.itaojin.util.StringUtils;
import com.qc.itaojin.entity.ZKNodeInfoWrapper;
import com.qc.itaojin.service.IZookeeperService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.net.InetAddress;
import java.util.List;

/**
 * @desc 观察者注册器
 * @author fuqinqin
 * @date 2018-07-05
 */
@Slf4j
@Data
public class WatcherRegister extends Thread {

    private IZookeeperService zookeeperService;

    private ZooKeeper zooKeeper;

    /**
     * 目录
     * */
    private String path;

    /**
     * 竞选成功后要执行的线程任务
     * */
    private List<? extends Runnable> tasks;

    /**
     * @desc 开启监听
     * @param ephemeralNode zk临时节点
     * @param tasks canal客户端任务列表
     * */
    public void enableListen(String ephemeralNode, List<? extends Runnable> tasks){
        this.path = ephemeralNode;
        this.tasks = tasks;
        this.start();
    }

    @Override
    public void run() {
        if(StringUtils.isBlank(getPath())){
            throw new IllegalArgumentException("path is null");
        }

        if(CollectionUtils.isEmpty(tasks)){
            throw new IllegalArgumentException("task list is empty");
        }

        ZooKeeper zooKeeper = ((ZookeeperServiceImpl)zookeeperService).getZooKeeper();

        try {
            // 创建监听回调
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    try {
                        // 节点变更类型
                        Event.EventType type = event.getType();
                        if(Event.EventType.NodeDeleted == type){
                            log.info("leader crash, campaign will begin");
                            ZKNodeInfoWrapper wrapper = ZKNodeInfoWrapper.build();
                            // 竞选成功，开启任务
                            if(zookeeperService.registerLeader(path, JsonUtil.toJson(wrapper).getBytes(ItaojinZKConstants.CHARSET))){
                                log.info("竞选leader成功，准备启动任务");
                                for (Runnable task : tasks) {
                                    new Thread(task).start();
                                }
                            }
                            // 竞选失败，重新注册监听
                            else{
                                log.info("竞选leader失败，继续监听");
                                zooKeeper.getData(path, this, null);
                                log.info("listen leader...");
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            // 注册监听者
            zooKeeper.getData(path, watcher , null);
            log.info("listen leader...");

            while (true) {
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            throw new RuntimeException("=== HA机制启动失败 ===");
        }
    }
}
