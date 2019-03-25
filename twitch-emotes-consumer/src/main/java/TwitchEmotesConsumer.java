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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

public class TwitchEmotesConsumer {

  public static void main(String args[]) throws IOException, InterruptedException {
    // Loading config file.
    Properties config = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (InputStream stream = loader.getResourceAsStream("config.properties")) {
      config.load(stream);
    } catch (IOException e) {
      throw new AssertionError("Invalid config, we can't recover from this.", e);
    }

    // Getting list of emotes to count.
    TwitchEmotesApiWrapper emotesApi = new TwitchEmotesApiWrapper(config.getProperty("twitchClientId"));
    ArrayList<String> emoteList = emotesApi.getAllEmotes();
    HashSet<String> emotes = new HashSet<>(emoteList);

    // Setting up Spark.
    SparkConf sparkConfig = new SparkConf()
        .setMaster(config.getProperty("sparkMaster"))
        .setAppName(config.getProperty("sparkAppName"));
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig,
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

    emoteTotalCount = emoteTotalCount.reduceByKey((i1, i2) -> i1 + i2);

    emoteTotalCount.print(1000);

    jssc.start();
    jssc.awaitTermination();
  }

}
