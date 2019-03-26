/* Copyright (c) 2019 Bartosz Białoskórski

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
================================================================================*/

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

public class TwitchEmotesConsumer {

  public static void main(String args[]) {
    // Loading config file.
    Properties config = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (InputStream stream = loader.getResourceAsStream("config.properties")) {
      config.load(stream);
    } catch (IOException e) {
      throw new AssertionError("Invalid config, can't recover from this.", e);
    }

    // Getting list of emotes to count.
    TwitchEmotesApiWrapper emotesApi =
        new TwitchEmotesApiWrapper(config.getProperty("twitchClientId"));
    ArrayList<String> emoteList = null;
    try {
      emoteList = emotesApi.getGlobalEmotes();
    } catch (IOException e) {
      // This call has to succeed in order for this application to proceed.
      throw new AssertionError(e);
    }
    HashSet<String> emotes = new HashSet<>(emoteList);

    // Setting up Spark and Cassandra connector.
    SparkConf sparkConfig = new SparkConf()
        .set("spark.cassandra.connection.host", config.getProperty("sparkCassandraConnectionHost"))
        .set("spark.cassandra.auth.username", config.getProperty("sparkCassandraAuthUsername"))
        .set("spark.cassandra.auth.password", config.getProperty("sparkCassandraAuthPassword"));
    JavaSparkContext jsc = new JavaSparkContext(config.getProperty("sparkMaster"),
        config.getProperty("sparkAppName"), sparkConfig);
    JavaStreamingContext jssc = new JavaStreamingContext(jsc,
        Durations.seconds(Integer.parseInt(config.getProperty("sparkStreamingSecondsDuration"))));

    // Making spark less verbose.
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    // Setting up Kafka.
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", config.getProperty("kafkaMetadataBrokerList"));
    kafkaParams.put("auto.offset.reset", config.getProperty("kafkaAutoOffsetReset"));
    Set<String> kafkaTopics = Collections.singleton(config.getProperty("kafkaTopic"));

    // Creating Kafka stream.
    JavaPairInputDStream<String, String> kafkaStream =
        KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
            StringDecoder.class, kafkaParams, kafkaTopics);

    // Spark processing starts here.
    //===========================================================================================//

    // Getting rid of kafka keys.
    JavaDStream<String> lines = kafkaStream.map(t -> t._2);

    // Lines formatting. Removing everything up to channel name which is preceded by hashtag and
    // removing first colon which is directly preceding first word of a message, this allows us to
    // correctly split lines into words later on.
    JavaDStream<String> formattedLines = lines.map(
        s -> s.substring(s.indexOf("#")).replaceFirst(":", ""));

    // Extracting channel name from each line. After this transformation first element of a pair
    // is the channel name to which message was sent and second element is the content of the
    // message.
    JavaPairDStream<String, String> channelLines = formattedLines.mapToPair(
        s -> new Tuple2<>(s.substring(1, s.indexOf(" ")), s.substring(s.indexOf(" ") + 1)));

    // Splitting each message into words.
    JavaPairDStream<String, String> channelWords =
        channelLines.flatMapValues(s -> Arrays.asList(s.split(" ")));

    // Filtering for words which are emotes.
    channelWords = channelWords.filter(t -> emotes.contains(t._2));

    // Creating pairs for reduction counting emote occurrences by channel.
    JavaPairDStream<Tuple2<String, String>, Integer> emoteCountsByChannel =
        channelWords.mapToPair(t -> new Tuple2<>(t, 1));

    // Counting emote occurrences in each channel.
    emoteCountsByChannel = emoteCountsByChannel.reduceByKey((i1, i2) -> i1 + i2);

    JavaPairDStream<String, Integer> emoteTotalCount = emoteCountsByChannel.mapToPair(
        t -> new Tuple2<>(t._1._2, t._2));

    // Creating DStream for Cassandra insertion.
    JavaDStream<Tuple5<UUID, String, String, Integer, Timestamp>> timestampedCountsByChannel =
        emoteCountsByChannel.map(t -> new Tuple5<>(UUID.randomUUID(), t._1._2, t._1._1, t._2,
            new Timestamp(Calendar.getInstance().getTimeInMillis())));


    // Saving emote counts by channel to Cassandra table: id, emote, channel, count, timestamp.
    CassandraStreamingJavaUtil.javaFunctions(timestampedCountsByChannel)
        .writerBuilder(config.getProperty("cassandraKeyspaceName"), "channel_emote_occurrences",
            CassandraJavaUtil.mapTupleToRow(UUID.class, String.class, String.class, Integer.class,
                Timestamp.class))
        .saveToCassandra();

    // Counting total occurrences of each emote across all channels.
    emoteTotalCount = emoteTotalCount.reduceByKey((i1, i2) -> i1 + i2);

    // Creating DStream for Cassandra insertion.
    JavaDStream<Tuple4<UUID, String, Integer, Timestamp>> timestampedTotalCount =
        emoteTotalCount.map(t -> new Tuple4<>(UUID.randomUUID(), t._1, t._2,
            new Timestamp(Calendar.getInstance().getTimeInMillis())));


    // Saving total emote counts to Cassandra table: id, emote, count, timestamp.
    CassandraStreamingJavaUtil.javaFunctions(timestampedTotalCount)
        .writerBuilder(config.getProperty("cassandraKeyspaceName"), "emote-occurrences",
            CassandraJavaUtil.mapTupleToRow(UUID.class, String.class, Integer.class,
                Timestamp.class))
        .saveToCassandra();

    jssc.start();
    try {
      jssc.awaitTermination();
    } catch (InterruptedException e) {
      Logger.getLogger(TwitchEmotesConsumer.class.getName()).error(e);
    }
  }

}
