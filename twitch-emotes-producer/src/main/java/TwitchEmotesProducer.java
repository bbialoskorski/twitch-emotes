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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class TwitchEmotesProducer {

  public static void main(String args[]) throws IOException {
    Runtime.getRuntime().addShutdownHook(new ServerShutdownHook());
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("batch.size", 300);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    try (Producer<String, String> kafkaProducer = new KafkaProducer<>(props)) {
      TwitchStreamsApiWrapper twitchApi = new TwitchStreamsApiWrapper();
      HashSet<String> streams = twitchApi.getLiveStreamNames(800);
      ExecutorService executor = Executors.newFixedThreadPool(800);
      ExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
      ReentrantLock lock = new ReentrantLock();
      Hashtable<String, Future> scrapers = new Hashtable<>();
      DataSender<String> dataSender = new ToKafkaDataSender<String, String>(kafkaProducer, "twitch-chat");

      Properties config = new Properties();
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try (InputStream stream = loader.getResourceAsStream("config.properties")) {
        config.load(stream);
      }



      IrcProducerTaskFactory scrapersFactory = new IrcForwardingToKafkaTaskFactory(kafkaProducer, "twitch-chat", config.getProperty("twitchIrcServerAddress"), Integer.parseInt(config.getProperty("twitchIrcServerPort")), (String) config.get("twitchIrcDaemon"), config.getProperty("twitchUsername"), config.getProperty("twitchOAuthToken"));
      for (String stream : streams) {
        scrapers.put(stream, executor.submit(scrapersFactory.createIrcProducer(stream)));
      }
      ((ScheduledExecutorService) scheduler).scheduleAtFixedRate(new IrcProducersMonitoringTask(scrapers, executor, scrapersFactory, lock), 10, 10, TimeUnit.SECONDS);
      while(true);
      /*
      for (int i = 0; i < 100; i++)
        kafkaProducer.send(new ProducerRecord<String, String>("twitch-chat", Integer.toString(i), Integer.toString(i)));
      */
    }

  }

}
