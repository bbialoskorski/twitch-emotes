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

import org.apache.kafka.clients.producer.Producer;

/**
 * Factory producing IrcChatMessagesForwardingTask runnables forwarding chat messages to Kafka
 * broker.
 */
public class IrcForwardingToKafkaTaskFactory implements IrcProducerTaskFactory {

  private final Producer<String, String> mKafkaProducer;
  private final String mTopic;
  private final String mIrcServerAddress;
  private final Integer mPort;
  private final String mDaemon;
  private final String mUsername;
  private final String mOAuthToken;

  /**
   * @param kafkaProducer producer for Kafka server
   * @param topic Kafka topic to which we want to publish scraped messages
   * @param ircServerAddress address of IRC server we want to scrape from
   * @param port IRC server port we're going to be connecting to
   * @param daemon daemon to which PONG messages should be sent to keep IRC server connection alive
   * @param username IRC server username
   * @param oAuthToken IRC server OAuth token
   */
  IrcForwardingToKafkaTaskFactory(Producer<String, String> kafkaProducer, String topic,
      String ircServerAddress, Integer port, String daemon, String username, String oAuthToken) {
    mKafkaProducer = kafkaProducer;
    mTopic = topic;
    mIrcServerAddress = ircServerAddress;
    mPort = port;
    mDaemon = daemon;
    mUsername = username;
    mOAuthToken = oAuthToken;
  }

  @Override
  public Runnable createIrcProducer(String channelName) {
    return new IrcChatMessagesForwardingTask(mIrcServerAddress, mPort, mDaemon, mUsername,
        mOAuthToken, channelName, new ToKafkaDataSender<>(mKafkaProducer, mTopic));
  }
}
