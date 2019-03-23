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

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;


/**
 * Monitors started irc producers and revives them in case of disconnect.
 */
public class IrcProducersMonitoringTask implements Runnable {

  private Map<String, Future> mProducerFutures;
  private ExecutorService mExecutor;
  private Lock mLock;
  private IrcProducerTaskFactory mTaskFactory;

  /**
   * @param producerFutures Map of irc producer futures keyed with channel names
   * @param executor Executor handling irc connections
   * @param taskFactory factory producing tasks scraping IRC channels
   * @param lock Lock for synchronization of access to Hashtable of irc connections
   */
  IrcProducersMonitoringTask(Map<String, Future> producerFutures, ExecutorService executor,
      IrcProducerTaskFactory taskFactory, Lock lock) {
    mProducerFutures = producerFutures;
    mExecutor = executor;
    mTaskFactory = taskFactory;
    mLock = lock;
  }

  @Override
  public void run() {
    mLock.lock();
    ArrayList<String> streamNames = new ArrayList<>(mProducerFutures.keySet());
    for (String stream : streamNames) {
      Future future = mProducerFutures.get(stream);
      if (future.isDone()) {
        mProducerFutures.remove(stream);
        future = mExecutor.submit(mTaskFactory.createIrcProducer(stream));
        mProducerFutures.put(stream, future);
      }
    }
    mLock.unlock();
  }

}
