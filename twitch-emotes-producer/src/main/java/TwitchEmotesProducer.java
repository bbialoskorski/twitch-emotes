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

  public static void main(String args[]) {

    // Loading config file.
    Properties config = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (InputStream stream = loader.getResourceAsStream("config.properties")) {
      config.load(stream);
    } catch (IOException e) {
      throw new AssertionError("Invalid config, we can't recover from this.", e);
    }

    // Configuring kafka producer.
    Properties props = new Properties();
    props.put("bootstrap.servers", config.getProperty("kafkaBootstrapServer"));
    props.put("batch.size", config.getProperty("kafkaBatchSize"));
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> kafkaProducer = new KafkaProducer<>(props);
      TwitchStreamsApiWrapper twitchApi =
          new TwitchStreamsApiWrapper(config.getProperty("twitchClientId"));
      ExecutorService executorPool =
          Executors.newFixedThreadPool(Integer.parseInt(config.getProperty("numOfProducerThreads")));
      ScheduledExecutorService monitoringTaskScheduler = Executors.newSingleThreadScheduledExecutor();
      ScheduledExecutorService managingTaskScheduler = Executors.newSingleThreadScheduledExecutor();
      Hashtable<String, Future> producerFutures = new Hashtable<>();
      ReentrantLock lock = new ReentrantLock();

      IrcProducerTaskFactory scrapersFactory = new IrcForwardingToKafkaTaskFactory(kafkaProducer,
          config.getProperty("twitchChatKafkaTopic"), config.getProperty("twitchIrcServerAddress"),
          Integer.parseInt(config.getProperty("twitchIrcServerPort")),
          (String) config.get("twitchIrcDaemon"), config.getProperty("twitchUsername"),
          config.getProperty("twitchOAuthToken"));
      monitoringTaskScheduler.scheduleAtFixedRate(new IrcProducersManagingTask(
          Integer.parseInt(config.getProperty("numOfProducers")), producerFutures,
          executorPool, scrapersFactory, twitchApi, lock), 0, 60, TimeUnit.MINUTES);
      managingTaskScheduler.scheduleAtFixedRate(new IrcProducersMonitoringTask(producerFutures,
          executorPool, scrapersFactory, lock), 10, 10, TimeUnit.SECONDS);

      Runtime.getRuntime().addShutdownHook(new ServerShutdownHook(executorPool,
          monitoringTaskScheduler, managingTaskScheduler, kafkaProducer));
  }

}
