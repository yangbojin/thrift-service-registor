package com.idss.thrift.registor;

import org.I0Itec.zkclient.ZkClient;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ZookeeperRegistor {
    private ZkClient zkClient;

    private String configPath = Constant.DEFAULT_CONFIG_PATH;

    public ZookeeperRegistor(ZkClient zkClient) {
        this.zkClient = zkClient;
        createBasePath();
    }

    public ZookeeperRegistor(ZkClient zkClient, String configPath) {
        this.zkClient = zkClient;
        this.configPath = configPath;
        createBasePath();
    }

    private void createBasePath(){
        if(!zkClient.exists(configPath)) this.zkClient.createPersistent(configPath,true);
    }

    public void regist(int port,String serviceName) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getLocalHost();
        String servicePath = configPath.concat("/").concat(serviceName);
        if(!zkClient.exists(servicePath)) this.zkClient.createPersistent(servicePath,true);
        String path = servicePath.concat("/").concat(inetAddress.getHostAddress());
        zkClient.createEphemeral(path ,true);
        zkClient.writeData(path,port);
    }

    public TServer registAndStart(int port, String serviceName, TProcessor processor) throws UnknownHostException, TTransportException {
        TServer server = new TNonblockingServer(new TNonblockingServer.Args(new TNonblockingServerSocket(port))
                .protocolFactory(new TCompactProtocol.Factory())
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory()));
        regist(port,serviceName);
        return server;
    }

    public void unRegist(String serviceName) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getLocalHost();
        String servicePath = configPath.concat("/").concat(serviceName).concat("/").concat(inetAddress.getHostAddress());
        zkClient.delete(servicePath);
    }
}
