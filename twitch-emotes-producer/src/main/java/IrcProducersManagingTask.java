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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

/**
 * Fetches new list of channel names, stops producers which are not included on that list and
 * starts new producers for channels which are on fetched list but were not already running.
 */
public class IrcProducersManagingTask implements Runnable {

  private final int mNumProducers;
  private final Map<String, Future> mProducerFutures;
  private final ExecutorService mExecutor;
  private final IrcProducerTaskFactory mTaskFactory;
  private final IrcChannelNamesProvider mChannelNamesProvider;
  private final Lock mLock;

  /**
   * @param numProducers amount of producers you want to run
   * @param producerFutures Map of producer task futures keyed with channel names
   * @param executor executor handling producers
   * @param taskFactory factory of producer tasks
   * @param channelNamesProvider provider of channel names to scrape
   * @param lock lock for synchronizing access to producer futures map
   */
  IrcProducersManagingTask(int numProducers, Map<String, Future> producerFutures,
      ExecutorService executor, IrcProducerTaskFactory taskFactory,
      IrcChannelNamesProvider channelNamesProvider, Lock lock) {
    mNumProducers = numProducers;
    mProducerFutures = producerFutures;
    mExecutor = executor;
    mTaskFactory = taskFactory;
    mChannelNamesProvider = channelNamesProvider;
    mLock = lock;
  }

  @Override
  public void run() {
    try {
      HashSet<String> channelNames = mChannelNamesProvider.getChannelNames(mNumProducers);

      mLock.lock();

      ArrayList<String> producers = new ArrayList<>(mProducerFutures.keySet());
      // Stopping producers from channels no longer on the list.
      for (String producer : producers) {
        if (!channelNames.contains(producer)) {
          if (mProducerFutures.get(producer).cancel(true)) {
            mProducerFutures.remove(producer);
          }
        }
        else {
          channelNames.remove(producer);
        }
      }
      // Starting producers for new channels.
      for (String channel : channelNames) {
        Future future = mExecutor.submit(mTaskFactory.createIrcProducer(channel));
        mProducerFutures.put(channel, future);
      }

      mLock.unlock();

    } catch (IOException e) {
      // This might be a network error, keep going.
      Logger.getLogger(IrcProducersManagingTask.class.getName()).warning(e.getMessage());
    }
  }

}
