package com.avengers.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SinkES {
	private static final Logger LOG = LoggerFactory.getLogger(SinkES.class);

	public static void main(String[] args) throws Exception {
		//模拟数据
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> socketTextStream = env.socketTextStream("your host", 8000, "\n");
		DataStream<Tuple2<String, String>> mapStream = socketTextStream.map(new MapFunction<String, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> map(String word) throws Exception {
				return new Tuple2<>("1_word", word);
			}
		});
		//获取ES的Sink
		ElasticsearchSink elasticsearchSink = getdElasticSearchSink("your host");

		//数据存入到ES
		mapStream.addSink(elasticsearchSink);

		env.execute("SinkES");
	}

	private static  ElasticsearchSink getdElasticSearchSink(String hosts) {
		List<HttpHost> httpHosts = new ArrayList<>();
		for (String host : hosts.split(",")){
			httpHosts.add(new HttpHost(host, 9200, "http"));
		}
		ElasticsearchSink.Builder<Tuple2<String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<Tuple2<String, String>>() {
					public IndexRequest createIndexRequest(Tuple2<String, String> element) {
						Map<String, String> json = new HashMap<>();
						json.put("key", element.f0);
						json.put("value", element.f1);
						return Requests.indexRequest()
								//索引的名称要小写(添加日志后才发现)
								.index("finksink")
								.type("doc")
								.source(json);
					}

					@Override
					public void process(Tuple2<String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);
		//缓冲区大小，例如:当Flink接收1条数据后存入到ES中
		esSinkBuilder.setBulkFlushMaxActions(1);
		return esSinkBuilder.build();
	}
}
