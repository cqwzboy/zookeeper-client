package com.qc.itaojin.autoconfiguration;

import com.qc.itaojin.common.WatcherRegister;
import com.qc.itaojin.common.ZookeeperFactory;
import com.qc.itaojin.config.ItaojinZookeeperConfig;
import com.qc.itaojin.service.IZookeeperService;
import com.qc.itaojin.service.ZookeeperServiceImpl;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by fuqinqin on 2018/7/3.
 */
@Configuration
@EnableConfigurationProperties(ItaojinZookeeperConfig.class)
@ConditionalOnClass({IZookeeperService.class, WatcherRegister.class})
@ConditionalOnProperty(prefix = "itaojin", value = "enabled", matchIfMissing = true)
public class ItaojinZKAutoConfiguration {

    @Autowired
    private ItaojinZookeeperConfig zkConfig;

    @Bean
    @ConditionalOnMissingBean
    public IZookeeperService zookeeperService(){
        IZookeeperService zookeeperService = new ZookeeperServiceImpl();
        ZooKeeper zooKeeper = ZookeeperFactory.getInstance().getZookeeper(5000, null, zkConfig);
        ((ZookeeperServiceImpl) zookeeperService).setZooKeeper(zooKeeper);
        return zookeeperService;
    }

    @Bean
    @ConditionalOnMissingBean
    public WatcherRegister watcherRegister(){
        WatcherRegister watcherRegister = new WatcherRegister();
        watcherRegister.setZookeeperService(zookeeperService());
        return watcherRegister;
    }

}
