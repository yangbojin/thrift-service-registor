package com.idss.thrift.registor;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * thrift服务连接池
 */
public class ServicePool {
    private ZkClient zkClient;
    private String serviceName;
    private Map<String, List<TProtocol>> ip_protocol_map = new HashMap<>();
    private List<String> iplist = new ArrayList<>();
    private Map<TProtocol, Boolean> usedMap = new HashMap<>();

    /**
     *
     * @param zkClient
     * @param serviceName 服务名称
     * @param ptcCntPerServ 与每个服务器保持的连接数量
     */
    public ServicePool(ZkClient zkClient, String serviceName, int ptcCntPerServ) {
        this.zkClient = zkClient;
        this.serviceName = serviceName;
        List<String> childrenPaths = zkClient.getChildren(Constant.DEFAULT_CONFIG_PATH.concat("/").concat(serviceName));
        init(childrenPaths, ptcCntPerServ);
        zkClient.subscribeChildChanges(Constant.DEFAULT_CONFIG_PATH.concat("/").concat(serviceName), new IZkChildListener() {
            public void handleChildChange(String s, List<String> list) throws Exception {
                init(list, ptcCntPerServ);
            }
        });
    }

    /**
     * 获取一个空闲的连接
     * @return
     */
    public TProtocol get() {
        int i = 0;
        while (i < 1000) {
            synchronized (this) {
                for (Map.Entry<String, List<TProtocol>> entry : ip_protocol_map.entrySet()) {
                    List<TProtocol> protocols = entry.getValue();
                    if (protocols != null) {
                        for (TProtocol protocol : protocols) {
                            Boolean used = usedMap.get(protocol);
                            if (used == null || !used) {
                                usedMap.put(protocol, true);
                                return protocol;
                            }
                        }
                    }
                }
            }
            i++;
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        throw new RuntimeException("no useable service");
    }

    /**
     * 释放连接
     * @param protocol
     */
    public void release(TProtocol protocol) {
        if (protocol == null) {
            return;
        }
        synchronized (protocol) {
            if (usedMap.containsKey(protocol)) {
                usedMap.put(protocol, false);
            }
        }
    }

    /**
     * 初始化，将zookeeper注册节点路径下的所有服务，按照参数创建链接
     * @param childrenPaths 注册节点的所有服务的ip节点信息
     * @param ptcCntPerServ 每台服务器创建的连接数量
     */
    private void init(List<String> childrenPaths, int ptcCntPerServ) {
        synchronized (this) {
            iplist = new ArrayList<>();
            Map<TProtocol, Boolean> oldUsedMap = usedMap;
            usedMap = new HashMap<>();
            Map<String, List<TProtocol>> oldMap = ip_protocol_map;
            ip_protocol_map = new HashMap<>();
            childrenPaths.forEach(ip -> {
                int port = zkClient.readData(Constant.DEFAULT_CONFIG_PATH.concat("/").concat(serviceName).concat("/").concat(ip));
                List<TProtocol> protocols = new ArrayList<>();
                List<TProtocol> tProtocols = oldMap.get(ip);
                if (tProtocols != null) {
                    protocols = tProtocols;
                } else {
                    for(int i = 0; i < ptcCntPerServ; i++){
                        TFramedTransport transport = new TFramedTransport(new TSocket(ip, port));
                        TProtocol protocol = new TCompactProtocol(transport);
                        protocols.add(protocol);
                    }
                }
                protocols.forEach(protocol -> {
                    TTransport tran = protocol.getTransport();
                    try {
                        if (!tran.isOpen()) {
                            tran.open();
                        }
                        Boolean b_used = oldUsedMap.get(protocol);
                        if(b_used == null) {
                            b_used = false;
                        }
                        usedMap.put(protocol, b_used);
                    } catch (TTransportException e) {
                        e.printStackTrace();
                    }
                });
                ip_protocol_map.put(ip, protocols);
                iplist.add(ip);
            });
        }
    }
}
