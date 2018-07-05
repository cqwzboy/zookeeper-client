package com.qc.itaojin.common;

import com.qc.itaojin.util.YamlUtil;
import com.qc.itaojin.config.ItaojinZookeeperConfig;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Map;

import static com.qc.itaojin.common.ZookeeperFactory.ParamsParser.parseZKServers;

/**
 * @desc 工厂类
 * @author fuqinqin
 * @date 2018-07-04
 */
public class ZookeeperFactory {

    private static ZookeeperFactory zookeeperFactory = new ZookeeperFactory();
    private ZookeeperFactory(){
        if(zookeeperFactory != null){
            throw new IllegalArgumentException("Single Factory do not support new by Construct");
        }
    }
    public static ZookeeperFactory getInstance(){
        return zookeeperFactory;
    }

    /**
     * 产生一个客户端连接
     * */
    public ZooKeeper getZookeeper(int timeout, Watcher watcher){
        try {
            return new ZooKeeper(parseZKServers(), timeout, watcher);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public ZooKeeper getZookeeper(int timeout, Watcher watcher, ItaojinZookeeperConfig zkConfig){
        try {
            return new ZooKeeper(parseZKServers(zkConfig.getQuorum(), zkConfig.getPort()), timeout, watcher);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static class ParamsParser{
        public static Map<String, Object> genParams(){
            if(YamlUtil.load("application-dev.yaml")!=null &&
                    YamlUtil.load("application-prod.yaml")!=null){
                Map<String, Object> parent = YamlUtil.load("application.yaml");
                if(parent == null){
                    throw new IllegalArgumentException("do not find application.yaml");
                }
                try{
                    String flag = (String) ((Map<String, Object>)((Map<String, Object>)parent.get("spring")).get("profiles")).get("active");
                    if("dev".equals(flag)){
                        return YamlUtil.load("application-dev.yaml");
                    }else if("prod".equals(flag)){
                        return YamlUtil.load("application-prod.yaml");
                    }else{
                        throw new IllegalArgumentException("spring.profiles.active must in ('dev', 'prod')");
                    }
                }catch (Exception e){
                    throw new IllegalArgumentException("there is no param spring.profiles.active in application.yaml");
                }
            }else{
                return YamlUtil.load("application.yaml");
            }
        }

        public static int parsePort(){
            try{
                Map<String, Object> params = genParams();
                Object port = ((Map<String, Object>)((Map<String, Object>)params.get("itaojin")).get("zookeeper")).get("port");
                if(port != null){
                    return (int) port;
                }

                return 2181;
            }catch (Exception e){
                throw new IllegalArgumentException("illegal zookeeper port");
            }
        }

        public static String parseQuorum(){
            try{
                Map<String, Object> params = genParams();
                Object port = ((Map<String, Object>)((Map<String, Object>)params.get("itaojin")).get("zookeeper")).get("quorum");
                if(port != null){
                    return (String) port;
                }

                return null;
            }catch (Exception e){
                throw new IllegalArgumentException("illegal zookeeper quorum");
            }
        }

        public static String parseZKServers(){
            StringBuilder zkServers = new StringBuilder();
            String quorum = parseQuorum();
            int port = parsePort();
            for (String s : quorum.split(",")) {
                zkServers.append(s).append(":").append(port).append(",");
            }

            zkServers.setLength(zkServers.length() - 1);

            return zkServers.toString();
        }

        public static String parseZKServers(String quorum, int port){
            StringBuilder zkServers = new StringBuilder();
            for (String s : quorum.split(",")) {
                zkServers.append(s).append(":").append(port).append(",");
            }

            zkServers.setLength(zkServers.length() - 1);

            return zkServers.toString();
        }
    }

}
