package com.github.simplesteph.kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ElasticSearchConsumer {

    //////////////////////////
    /////////// IF YOU USE LOCAL ELASTICSEARCH
    //////////////////////////

    //  String bonsaiHostname = "localhost";
    //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


    //////////////////////////
    /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
    //////////////////////////

    static String bonsaiHostname;
    static String bonsaiUsername;
    static String bonsaiPassword;

    static void setUpBonsaiConfig() throws IOException {
        Properties config = new Properties();
        final InputStream istream;
        try { istream = new FileInputStream("novcs-bonsai-credentials.properties");
        } catch (FileNotFoundException e) {
            throw e;
        }
        config.load(istream);
        bonsaiHostname = config.getProperty("hostname");
        bonsaiUsername = config.getProperty("username");
        bonsaiPassword = config.getProperty("password");
    }


    public static RestHighLevelClient createClient() throws IOException {

        setUpBonsaiConfig();
        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(bonsaiUsername, bonsaiPassword));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(bonsaiHostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        String jsonString = "{ \"foo\": \"bar\"}";

        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "tweets"
        ).source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info(id);

        client.close();
    }
}
