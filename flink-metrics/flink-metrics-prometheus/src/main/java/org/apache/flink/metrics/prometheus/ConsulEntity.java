package org.apache.flink.metrics.prometheus;

import java.util.List;

/**
 * @Author: Franksen
 * @Program: org.apache.flink.metrics.prometheus
 * @Date: 2023/7/7 17:08
 * @Description:
 */
public class ConsulEntity {

    //curl -X PUT -d
    // '{"id": "node-exporter",
    // "name": "node-exporter",
    // "address": "192.168.253.69",
    // "port": 9100,
    // "tags": ["test"],
    // "checks": [{"http": "http://192.168.253.69:9100/metrics", "interval": "5s"}]}'
    // http://192.168.253.69:8500/v1/agent/service/register

    private String id;

    private String name;

    private String address;

    private Integer port;

    private List<String> tags;

    private List<String> checks;

    public ConsulEntity(String id, String name, String address, Integer port, List<String> tags) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.port = port;
        this.tags = tags;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public List<String> getChecks() {
        return checks;
    }

    public void setChecks(List<String> checks) {
        this.checks = checks;
    }
}
