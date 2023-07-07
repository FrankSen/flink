/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.prometheus;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.agent.model.Check;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.agent.model.Service;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus. */
@PublicEvolving
@InstantiateViaFactory(
        factoryClassName = "org.apache.flink.metrics.prometheus.PrometheusReporterFactory")
public class PrometheusReporter extends AbstractPrometheusReporter {

    private final String ARG_PORT = "port";
    private final String ARG_CONSUL_HOST = "consulHost";
    private final String ARG_CONSUL_PORT = "consulPort";
    private final String ARG_CONSUL_TAGS = "consulTags";
    private static final String DEFAULT_PORT = "9248";
    private static final String ID_FORMAT = "flink_metrics_%s_%s";
    private ConsulClient consulClient;
    private AtomicInteger atomicInteger = new AtomicInteger();
    private HTTPServer httpServer;
    private int port;


    @VisibleForTesting
    int getPort() {
        Preconditions.checkState(httpServer != null, "Server has not been initialized.");
        return port;
    }

    PrometheusReporter(MetricConfig config, Iterator<Integer> ports, String portsConfig) {
        while (ports.hasNext()) {
            int port = ports.next();
            try {
                // internally accesses CollectorRegistry.defaultRegistry
                httpServer = new HTTPServer(port);
                this.port = port;
                log.info("Started PrometheusReporter HTTP server on port {}.", port);
                try {
                    if (!config.containsKey(ARG_CONSUL_HOST) || !config.containsKey(ARG_CONSUL_PORT)) {
                        log.warn("Consul host or port can't be null");
                        break;
                    }
                    String consulHost = config.getString(ARG_CONSUL_HOST, null);
                    int consulPort = config.getInteger(ARG_CONSUL_PORT, 0);
                    this.consulClient = new ConsulClient(consulHost, consulPort);
                    Set<Map.Entry<String, Service>> entrySet = consulClient.getAgentServices().getValue().entrySet();
                    int max = 0;
                    if (entrySet.size() == 0) {
                        atomicInteger.set(1);
                    } else {
                        for (Map.Entry<String, Service> entry : entrySet) {
                            String serviceName = entry.getValue().getService();
                            String str = serviceName.substring(serviceName.lastIndexOf("_") + 1);
                            int index = 0;
                            try {
                                index = Integer.parseInt(str);
                            } catch (Exception e) {
                                log.warn("Consul  error", e);
                                index = 0;
                            }
                            if (max < index) {
                                max = index;
                            }
                        }
                        atomicInteger.set(max + 1);
                    }
                    String hostAddress = InetAddress.getLocalHost().getHostAddress();
                    NewService newService = new NewService();
                    newService.setId(String.format(ID_FORMAT, hostAddress, port));
                    String name = "flink_metrics_" + atomicInteger.get();
                    newService.setName(name);
                    newService.setPort(port);
                    newService.setAddress(hostAddress);
                    NewService.Check check = new NewService.Check();
                    String url = "http://" + hostAddress + ":" + port + "/metrics";
                    log.warn("Consulclient check url{}", url);
                    check.setHttp(url);
                    check.setInterval("5s");
                    newService.setCheck(check);
                    consulClient.agentServiceRegister(newService);
                    atomicInteger.getAndIncrement();
                    break;
                } catch (Exception e) {
                    log.warn("Consul Entity register error", e);
                }
            } catch (IOException ioe) { // assume port conflict
                log.debug("Could not start PrometheusReporter HTTP server on port {}.", port, ioe);
            }
        }

        if (httpServer == null) {
            throw new RuntimeException(
                    "Could not start PrometheusReporter HTTP server on any configured port. Ports: "
                            + ports);
        }
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.stop();
        }
        log.warn("close method exec...");
        try {
            if (this.consulClient != null){
                Thread.sleep(8000);

                for (Map.Entry<String, Check> entry : consulClient
                        .getAgentChecks()
                        .getValue()
                        .entrySet()) {
                    Check check = entry.getValue();
                    String serviceName = check.getServiceName();
                    log.info("serviceName:{}", serviceName);
                    if (check.getStatus() == Check.CheckStatus.CRITICAL) {
                        log.info("服务：{}为无效服务，准备清理....", serviceName);
                        consulClient.agentServiceDeregister(check.getServiceId());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Consul entity deregister error", e);
        }
        super.close();
    }

}
