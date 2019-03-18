package com.idss.thrift.registor;

import org.I0Itec.zkclient.ZkClient;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 服务注册器
 */
public class ZookeeperRegistor {
    private ZkClient zkClient;

    private String configPath = Constant.DEFAULT_CONFIG_PATH;

    /**
     * 构造函数
     * @param zkClient zookeeper客户端
     */
    public ZookeeperRegistor(ZkClient zkClient) {
        this.zkClient = zkClient;
        createBasePath();
    }

    /**
     * 构造函数
     * @param zkClient zookeeper客户端
     * @param configPath 配置路径
     */
    public ZookeeperRegistor(ZkClient zkClient, String configPath) {
        this.zkClient = zkClient;
        this.configPath = configPath;
        createBasePath();
    }

    private void createBasePath(){
        if(!zkClient.exists(configPath)) this.zkClient.createPersistent(configPath,true);
    }

    /**
     * 将服务注册到zookeeper上，节点值为端口号，节点名为服务名/ip地址
     * @param port 端口号
     * @param serviceName 服务名称
     * @throws UnknownHostException
     */

    public void regist(int port,String serviceName) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getLocalHost();
        String servicePath = configPath.concat("/").concat(serviceName);
        if(!zkClient.exists(servicePath)) this.zkClient.createPersistent(servicePath,true);
        String path = servicePath.concat("/").concat(inetAddress.getHostAddress());
        zkClient.createEphemeral(path ,true);
        zkClient.writeData(path,port);
    }

    /**
     * 注册服务并启动
     * @param port  端口号
     * @param serviceName  服务名称
     * @param processor  处理器
     * @param threadCnt  线程数
     * @throws UnknownHostException
     * @throws TTransportException
     */
    public void registAndStart(int port, String serviceName, TProcessor processor,int threadCnt) throws UnknownHostException, TTransportException {
        int threadCount = 1;
        if(threadCnt > 1) {
            threadCount = threadCnt;
        }
        TServer server = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(new TNonblockingServerSocket(port))
                .protocolFactory(new TCompactProtocol.Factory())
                .processor(processor)
                .workerThreads(threadCount)
                .transportFactory(new TFramedTransport.Factory()));
        regist(port,serviceName);
        server.serve();
    }

    /**
     * 注销服务（服务器停止时使用）
     * @param serviceName 服务名称
     * @throws UnknownHostException
     */
    public void unRegist(String serviceName) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getLocalHost();
        String servicePath = configPath.concat("/").concat(serviceName).concat("/").concat(inetAddress.getHostAddress());
        zkClient.delete(servicePath);
    }
}
