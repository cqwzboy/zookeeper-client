package com.qc.itaojin.service;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;

/**
 * @desc Zookeeper服务类
 * @author fuqinqin
 * @date 2018-07-04
 */
public interface IZookeeperService {

    /**
     * @desc 增加节点
     * @param path 目录
     * @param content 值
     * @param acl 权限，默认全部权限
     * @param createMode 节点类型
     * */
    boolean create(String path, byte[] content, ArrayList<ACL> acl, CreateMode createMode);
    boolean create(String path, byte[] content, CreateMode createMode);
    boolean create(String path, byte[] content, ArrayList<ACL> acl);
    boolean create(String path, byte[] content);

    /**
     * @desc 赋值/修改
     * @param path 目录
     * @param content 内容
     * @param version 版本号
     * */
    boolean setData(String path, byte[] content, int version);

    /**
     * @desc 删除
     * @param path 目录
     * @param version 版本号
     * */
    boolean delete(String path, int version);

    /**
     * @desc 查询指定目录下的所有节点
     * @param path 目标目录
     * @param recursive 是否递归
     * */
    List<String> queryAllNodes(String path, boolean recursive);

    /**
     * @desc 查询某个节点的值
     * @param path 目标目录
     * @param watcher 观察者
     * @param stat 节点状态值对象
     * */
    byte[] getData(String path, Watcher watcher, Stat stat);
    byte[] getData(String path);

    /**
     * @desc 注册leader
     * @param path 目标目录
     * @param data z节点值
     * */
    boolean registerLeader(String path, byte[] data);

}
