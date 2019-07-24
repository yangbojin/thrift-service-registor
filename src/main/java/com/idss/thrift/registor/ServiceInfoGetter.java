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
    private Map<String, Integer> ip_port_map;
    private List<String> ips = new ArrayList<>();
    private String serviceName;
    private String zkBasePath;

    public ServiceInfoGetter(ZkClient zkClient, String serviceName) {
        this(zkClient, serviceName, null);
    }

    public ServiceInfoGetter(ZkClient zkClient, String serviceName, String zkBasePath) {
        if (zkBasePath == null || zkBasePath.trim().length() == 0) {
            this.zkBasePath = Constant.DEFAULT_CONFIG_PATH;
        } else {
            this.zkBasePath = zkBasePath.trim();
        }
        this.zkClient = zkClient;
        this.serviceName = serviceName;
        init(zkClient.getChildren(zkBasePath.concat("/").concat(serviceName)));
        zkClient.subscribeChildChanges(zkBasePath.concat("/").concat(serviceName), new IZkChildListener() {
            public void handleChildChange(String s, List<String> list) throws Exception {
                init(list);
            }
        });
    }

    public ServiceConfig getRandomConfig() {
        String ip = ips.get(new Random().nextInt(ips.size()));
        Integer port = ip_port_map.get(ip);
        return new ServiceConfig(ip, port);
    }

    private void init(List<String> ips) {
        synchronized (this) {
            ip_port_map = new ConcurrentHashMap<>();
            for (String ip : ips) {
                int port = zkClient.readData(zkBasePath.concat("/").concat(serviceName).concat("/").concat(ip));
                ip_port_map.put(ip, port);
            }
            this.ips = ips;
        }
    }
}
