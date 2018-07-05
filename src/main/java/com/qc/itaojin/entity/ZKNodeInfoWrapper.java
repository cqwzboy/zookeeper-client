package com.qc.itaojin.entity;

import com.qc.itaojin.util.InetAddressUtil;
import lombok.Data;

/**
 * Created by fuqinqin on 2018/7/4.
 */
@Data
public class ZKNodeInfoWrapper {

    private boolean active;

    private String ip;

    private String hostname;

    public static ZKNodeInfoWrapper build(){
        ZKNodeInfoWrapper wrapper = new ZKNodeInfoWrapper();
        wrapper.setActive(true);
        wrapper.setIp(InetAddressUtil.getHostAddress());
        wrapper.setHostname(InetAddressUtil.getHostName());
        return wrapper;
    }

}
