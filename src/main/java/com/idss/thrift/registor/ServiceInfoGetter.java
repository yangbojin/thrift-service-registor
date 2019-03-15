package com.idss.thrift.registor;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceInfoGetter {
    private ZkClient zkClient;
    private Map<String,Integer> ip_port_map;
    private List<String> ips = new ArrayList<>();
    private String serviceName;

    public ServiceInfoGetter(ZkClient zkClient, String serviceName) {
        this.zkClient = zkClient;
        this.serviceName = serviceName;
        init(zkClient.getChildren(Constant.DEFAULT_CONFIG_PATH.concat("/").concat(serviceName)));
        zkClient.subscribeChildChanges(Constant.DEFAULT_CONFIG_PATH.concat("/").concat(serviceName), new IZkChildListener() {
            public void handleChildChange(String s, List<String> list) throws Exception {
                init(list);
            }
        });
    }

    public ServiceConfig getRandomConfig(){
        String ip = ips.get(new Random().nextInt(ips.size()));
        Integer port = ip_port_map.get(ip);
        return new ServiceConfig(ip,port);
    }

    private void init(List<String> ips){
        synchronized(this){
            ip_port_map = new ConcurrentHashMap<>();
            for (String ip : ips) {
                int port = zkClient.readData(Constant.DEFAULT_CONFIG_PATH.concat("/").concat(serviceName).concat("/").concat(ip));
                ip_port_map.put(ip,port);
            }
            this.ips = ips;
        }
    }
}
