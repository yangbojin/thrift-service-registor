package com.idss.thrift.registor;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.Watcher;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 服务注册器
 */
public class ZookeeperRegistor {
    private ZkClient zkClient;

    private String configPath = Constant.DEFAULT_CONFIG_PATH;

    /**
     * 构造函数
     *
     * @param zkClient zookeeper客户端
     */
    public ZookeeperRegistor(ZkClient zkClient) {
        this.zkClient = zkClient;
        createBasePath();
    }

    /**
     * 构造函数
     *
     * @param zkClient   zookeeper客户端
     * @param configPath 配置路径
     */
    public ZookeeperRegistor(ZkClient zkClient, String configPath) {
        this.zkClient = zkClient;
        this.configPath = configPath;
        createBasePath();
    }

    private void createBasePath() {
        if (!zkClient.exists(configPath)) this.zkClient.createPersistent(configPath, true);
    }

    /**
     * 将服务注册到zookeeper上，节点值为端口号，节点名为服务名/ip地址
     *
     * @param port        端口号
     * @param serviceName 服务名称
     * @throws UnknownHostException
     */
    private Set<NodeData> nodeDataSet = ConcurrentHashMap.newKeySet();
    public void regist(int port, String serviceName) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getLocalHost();
        String servicePath = configPath.concat("/").concat(serviceName);
        if (!zkClient.exists(servicePath)) this.zkClient.createPersistent(servicePath, true);
        String path = servicePath.concat("/").concat(inetAddress.getHostAddress());
        zkClient.createEphemeral(path, true);
        zkClient.writeData(path, port);
        nodeDataSet.add(new NodeData(path,port));
    }

    private static class NodeData{
        String path;
        int port;
        public NodeData(String path, int port) {
            this.path = path;
            this.port = port;
        }
    }

    private void watchChange() {
        zkClient.subscribeStateChanges(new IZkStateListener() {
            @Override
            public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
                if (keeperState.equals(Watcher.Event.KeeperState.Expired)) {
                    nodeDataSet.forEach(item -> {
                        zkClient.createEphemeral(item.path, true);
                        zkClient.writeData(item.path, item.port);
                    });
                }
            }

            @Override
            public void handleNewSession() throws Exception {

            }

            @Override
            public void handleSessionEstablishmentError(Throwable throwable) throws Exception {

            }
        });
    }

    /**
     * 注册服务并启动
     *
     * @param port        端口号
     * @param serviceName 服务名称
     * @param processor   处理器
     * @param threadCnt   线程数
     * @throws UnknownHostException
     * @throws TTransportException
     */
    public TServer registAndStart(int port, String serviceName, TProcessor processor, int threadCnt) throws TTransportException, UnknownHostException {
        int threadCount = 1;
        if (threadCnt > 1) {
            threadCount = threadCnt;
        }
        TServer server = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(new TNonblockingServerSocket(port))
                .protocolFactory(new TCompactProtocol.Factory())
                .processor(processor)
                .workerThreads(threadCount)
                .transportFactory(new TFramedTransport.Factory()));
        regist(port, serviceName);
        watchChange();
        return server;
    }

    /**
     * 注销服务（服务器停止时使用）
     *
     * @param serviceName 服务名称
     * @throws UnknownHostException
     */
    public void unRegist(String serviceName) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getLocalHost();
        String servicePath = configPath.concat("/").concat(serviceName).concat("/").concat(inetAddress.getHostAddress());
        zkClient.delete(servicePath);
    }
}
