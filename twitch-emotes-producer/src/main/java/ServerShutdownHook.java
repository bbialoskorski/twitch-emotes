import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
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
      mMonitoringTaskScheduler.shutdownNow();
      mManagingTaskScheduler.shutdownNow();
      mThreadPool.shutdownNow();
      mKafkaProducer.close();
  }
}
