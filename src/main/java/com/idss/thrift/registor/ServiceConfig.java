package com.idss.thrift.registor;

public class ServiceConfig {
    private String ip;
    private int port;

    public ServiceConfig(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "ServiceConfig{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }
}
