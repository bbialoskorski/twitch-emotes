import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.Producer;

/**
 * Shutdowns executors and closes Kafka producer.
 */
public class ServerShutdownHook extends Thread {

  private ExecutorService mThreadPool;
  private ScheduledExecutorService mMonitoringTaskScheduler;
  private ScheduledExecutorService mManagingTaskScheduler;
  private Producer<String, String> mKafkaProducer;

  /**
   * @param threadPool thread pool executing producers
   * @param monitoringTaskScheduler scheduled executor executing monitoring task
   * @param managingTaskScheduler scheduled executor executing managing task
   * @param kafkaProducer Kafka producer
   */
  ServerShutdownHook(ExecutorService threadPool, ScheduledExecutorService monitoringTaskScheduler,
  ScheduledExecutorService managingTaskScheduler, Producer<String, String> kafkaProducer) {
    mThreadPool = threadPool;
    mMonitoringTaskScheduler = monitoringTaskScheduler;
    mManagingTaskScheduler = managingTaskScheduler;
    mKafkaProducer = kafkaProducer;
  }

  @Override
  public void run() {
    try {
      mManagingTaskScheduler.shutdownNow();
      mManagingTaskScheduler.awaitTermination(1000, TimeUnit.SECONDS);
      mMonitoringTaskScheduler.shutdownNow();
      mMonitoringTaskScheduler.awaitTermination(1000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Logger.getLogger(ServerShutdownHook.class.getName()).warning(e.getMessage());
    }
    mThreadPool.shutdownNow();
    mKafkaProducer.close();
    Logger.getLogger(ServerShutdownHook.class.getName()).info("Server finished.");
  }
}
