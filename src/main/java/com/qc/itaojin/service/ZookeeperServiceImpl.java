package com.qc.itaojin.service;

import com.qc.itaojin.util.StringUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by fuqinqin on 2018/7/4.
 */
@Data
@Slf4j
public class ZookeeperServiceImpl extends BaseServiceImpl implements IZookeeperService {

    private ZooKeeper zooKeeper;

    @Override
    public boolean create(String path, byte[] content, ArrayList<ACL> acl, CreateMode createMode) {
        try {
            if(zooKeeper.exists(path, false) != null){
                return true;
            }

            zooKeeper.create(path, content, acl, createMode);
            return true;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public boolean create(String path, byte[] content, CreateMode createMode) {
        create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        return true;
    }

    @Override
    public boolean create(String path, byte[] content, ArrayList<ACL> acl) {
        create(path, content, acl, CreateMode.PERSISTENT);
        return true;
    }

    @Override
    public boolean create(String path, byte[] content) {
        create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        return true;
    }

    @Override
    public boolean setData(String path, byte[] content, int version) {
        try {
            zooKeeper.setData(path, content, version);
            return true;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean delete(String path, int version) {
        try {
            zooKeeper.delete(path, version);
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public List<String> queryAllNodes(String path, boolean recursive) {
        try {
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
        try {
            return zooKeeper.getData(path, watcher, stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public byte[] getData(String path) {
        try {
            return zooKeeper.getData(path, null, null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public boolean registerLeader(String path, byte[] data) {
        // 初始化父节点目录
        initDirectory(path);

        // 创建临时节点进行leader选举
        try {
            if(zooKeeper.exists(path, false) != null){
                return false;
            }

            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 初始化竞争临时节点的父节点
     * */
    private void initDirectory(String path) {
        Assert.hasLength(path, "zookeeper inition directory is null");
        if(path.indexOf("/") == -1){
            throw new IllegalArgumentException("zookeeper inition directory must contains '/'");
        }

        String[] paths = path.split("/");
        String p = "";
        // 忽略最后一层临时节点
        for (int i=0;i<paths.length-1;i++) {
            String node = paths[i];
            if(StringUtils.isBlank(node)){
                continue;
            }
            p = StringUtils.contact(p, "/", node);
            create(p, null);
        }
    }
}
