package com.qc.itaojin.service;

import com.qc.itaojin.common.ZookeeperFactory;
import com.qc.itaojin.config.ItaojinZookeeperConfig;
import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by fuqinqin on 2018/7/4.
 */
@Data
@Slf4j
public class ZookeeperServiceImpl2 implements IZookeeperService {

    private ItaojinZookeeperConfig zkConfig;

    @Override
    public boolean create(String path, byte[] content, ArrayList<ACL> acl, CreateMode createMode) {
        ZooKeeper zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
        try {
            zooKeeper.create(path, content, acl, createMode);
            return true;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(zooKeeper != null){
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {

                }
            }
        }

        return false;
    }

    @Override
    public boolean create(String path, byte[] content, CreateMode createMode) {
        boolean flag;
        ZooKeeper zooKeeper = null;
        try{
            zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
            flag = create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        }finally {
            if(zooKeeper != null){
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {

                }
            }
        }

        return flag;
    }

    @Override
    public boolean create(String path, byte[] content, ArrayList<ACL> acl) {
        boolean flag;
        ZooKeeper zooKeeper = null;
        try{
            zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
            flag = create(path, content, acl, CreateMode.PERSISTENT);
        }finally {
            if(zooKeeper != null){
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {

                }
            }
        }

        return flag;
    }

    @Override
    public boolean create(String path, byte[] content) {
        boolean flag;
        ZooKeeper zooKeeper = null;
        try{
            zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
            flag = create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }finally {
            if(zooKeeper != null){
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {

                }
            }
        }

        return flag;
    }

    @Override
    public boolean setData(String path, byte[] content, int version) {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
            zooKeeper.setData(path, content, version);
            return true;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(zooKeeper != null){
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {

                }
            }
        }

        return false;
    }

    @Override
    public boolean delete(String path, int version) {
        ZooKeeper zooKeeper = null;
        try{
            zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
            zooKeeper.delete(path, version);
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {
            if(zooKeeper != null){
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {

                }
            }
        }

        return false;
    }

    @Override
    public List<String> queryAllNodes(String path, boolean recursive) {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
            if(!recursive){
                return zooKeeper.getChildren(path, false);
            }else{
                List<String> list = new ArrayList<>();
                queryAll(path, list, zooKeeper);
                return list;
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(zooKeeper != null){
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {

                }
            }
        }

        return null;
    }

    /**
     * 递归查询指定目录下的所有子目录
     * */
    private void queryAll(String path, List<String> list, ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(path, false);
        if (children == null || children.isEmpty()) {
            return ;
        }
        for (String child : children) {
            list.add(child);
            if ("/".equals(path)) {
                queryAll(StringUtils.contact(path, child), list, zooKeeper);
            } else {
                queryAll(StringUtils.contact(path, "/", child), list, zooKeeper);
            }
        }
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat) {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
            return zooKeeper.getData(path, watcher, stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(zooKeeper != null){
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {

                }
            }
        }

        return null;
    }

    @Override
    public byte[] getData(String path) {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
            return zooKeeper.getData(path, null, null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(zooKeeper != null){
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {

                }
            }
        }

        return null;
    }

    @Override
    public boolean registerLeader(String path, byte[] data) {
        // 创建临时节点进行leader选举
//        return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        ZooKeeper zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
        try {
            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }
}
