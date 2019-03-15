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
    private Map<String,TProtocol> ip_protocol_map = new HashMap<>();
    private List<String> iplist = new ArrayList<>();
    private Map<TProtocol,Boolean> usedMap = new HashMap<>();

    public ServicePool(ZkClient zkClient, String serviceName) {
        this.zkClient = zkClient;
        this.serviceName = serviceName;
        List<String> childrenPaths = zkClient.getChildren(Constant.DEFAULT_CONFIG_PATH.concat("/").concat(serviceName));
        init(childrenPaths);
        zkClient.subscribeChildChanges(Constant.DEFAULT_CONFIG_PATH.concat("/").concat(serviceName), new IZkChildListener() {
            public void handleChildChange(String s, List<String> list) throws Exception {
                init(childrenPaths);
            }
        });
    }

    public TProtocol get(){
        int i = 0;
        while ( i< 1000){
            synchronized (this){
                for (Map.Entry<String, TProtocol> entry : ip_protocol_map.entrySet()) {
                    TProtocol protocol = entry.getValue();
                    Boolean used = usedMap.get(protocol);
                    if(used == null || !used){
                        usedMap.put(protocol,true);
                        return protocol;
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
        return null;
    }

    public void release(TProtocol protocol){
        if(protocol == null) {
            return;
        }
        synchronized (protocol){
            if(usedMap.containsKey(protocol)){
                usedMap.put(protocol,false);
            }
        }
    }

    private void init(List<String> childrenPaths){
        synchronized (this){
            iplist = new ArrayList<>();
            Map<TProtocol,Boolean> oldUsedMap = usedMap;
            usedMap = new HashMap<>();
            Map<String,TProtocol> oldMap = ip_protocol_map;
            ip_protocol_map = new HashMap<>();
            childrenPaths.forEach(ip -> {
                int port = zkClient.readData(Constant.DEFAULT_CONFIG_PATH.concat("/").concat(serviceName).concat("/").concat(ip));
                TFramedTransport transport = new TFramedTransport(new TSocket(ip, port));
                TProtocol protocol = new TCompactProtocol(transport);
                try {
                    boolean used = false;
                    TProtocol tProtocol = oldMap.get(ip);
                    if(tProtocol != null){
                        protocol = tProtocol;
                        used = oldUsedMap.get(protocol);
                    }
                    TTransport tran = protocol.getTransport();
                    if(!tran.isOpen()){
                        tran.open();
                    }
                    ip_protocol_map.put(ip,protocol);
                    usedMap.put(protocol,used);
                    iplist.add(ip);
                } catch (TTransportException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
