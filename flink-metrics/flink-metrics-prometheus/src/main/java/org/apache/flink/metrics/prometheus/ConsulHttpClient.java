package org.apache.flink.metrics.prometheus;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * @Author: Franksen
 * @Program: org.apache.flink.metrics.prometheus
 * @Date: 2023/7/7 17:10
 * @Description:
 */
public class ConsulHttpClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulHttpClient.class);

    private static final MediaType MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");
    private static final int TIMEOUT = 3;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String CONSUL_ADDRESS_FORMAT = "%s:%d";
    private static final String REGISTER_URL_FORMAT = "http://%s/v1/agent/service/register";
    private static final String DEREGISTER_URL_FORMAT = "http://%s/v1/agent/service/deregister/%s";

    private final OkHttpClient client;
    private final ConsulEntity entity;

    private final String consulAddress;

    private static final String ID_FORMAT = "flink_metrics_%s_%d";
    private static final String NAME = "flink_metrics";

    public ConsulHttpClient(String consulHost, int consulPort, String host, int port, List<String> tags) {
        client = new OkHttpClient.Builder()
                .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
                .writeTimeout(TIMEOUT, TimeUnit.SECONDS)
                .readTimeout(TIMEOUT, TimeUnit.SECONDS)
                .build();

        entity = new ConsulEntity(String.format(ID_FORMAT, host, port),
                NAME,
                host,
                port,
                tags);

        consulAddress = String.format(CONSUL_ADDRESS_FORMAT, consulHost, consulPort);
    }

    public ConsulEntity getEntity() {
        return entity;
    }

    public String getConsulAddress() {
        return consulAddress;
    }

    public void register() throws Exception {
        this.send(String.format(REGISTER_URL_FORMAT, this.consulAddress), this.entity);
        //list.add(this.consulAddress + "&" + this.entity.getId());
        LOGGER.warn("register consulAddress {} entity_id {}", this.consulAddress, this.entity.getId());
    }

    public void deregister() throws Exception {
        //LOGGER.warn("deregister list size {}", list.size());
        //for (int i = 0; i < list.size(); i++)
        {
            //String[] arr = list.get(i).split("&");
            //LOGGER.warn("deregister consulAddress {} entity_id {}", arr[0], arr[1]);
            this.send(String.format(DEREGISTER_URL_FORMAT, this.consulAddress, this.entity.getId()), null);
        }
    }

    public void send(String consulUrl, ConsulEntity request) throws Exception {
        String postBody = serialize(request);

        Request r = new Request.Builder()
                .url(consulUrl)
                .put(RequestBody.create(MEDIA_TYPE, postBody))
                .build();

        client.newCall(r).enqueue(EmptyCallback.getEmptyCallback());
    }

    public static String serialize(Object obj) throws JsonProcessingException {
        if (obj == null) {
            return "";
        }
        return MAPPER.writeValueAsString(obj);
    }

    public void close() {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
        //list.clear();
    }

    /**
     * A handler for OkHttpClient callback.  In case of error or failure it logs the error at warning level.
     */
    protected static class EmptyCallback implements Callback {

        private static final EmptyCallback singleton = new EmptyCallback();

        public static Callback getEmptyCallback() {
            return singleton;
        }

        @Override
        public void onFailure(Call call, IOException e) {
            LOGGER.warn("Failed sending request to Consul", e);
        }

        @Override
        public void onResponse(Call call, Response response) throws IOException {
            if (!response.isSuccessful()) {
                LOGGER.warn("Failed to send request to Consul (response was {})", response);
            }

            response.close();
        }
    }

}
