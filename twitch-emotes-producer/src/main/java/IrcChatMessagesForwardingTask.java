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
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Connects to twitch.tv channel and forwards chat messages to a consumer.
 */
public class IrcChatMessagesForwardingTask implements Runnable {

  private final String mChannelName;
  private DataSender<String> mDataSender;

  /**
   * @param channelName name of twitch.tv channel you want to forward messages from
   * @param dataSender dataSender forwarding chat messages
   */
  IrcChatMessagesForwardingTask(String channelName, DataSender<String> dataSender) {
    mChannelName = channelName;
    mDataSender = dataSender;
  }

  @Override
  public void run() {
    Properties config = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    try (InputStream stream = loader.getResourceAsStream("config.properties")) {
      config.load(stream);
    } catch (IOException e) {
      // Invalid config file, we can't recover from this.
      throw new AssertionError("Invalid config.properties file, can't recover from this.", e);
    }

    try (Socket socket = new Socket(config.getProperty("twitchIrcServerAddress"),
        Integer.parseInt(config.getProperty("twitchIrcServerPort")))) {

      TwitchIrcWriterProxy ircWriter = new TwitchIrcWriterProxy(socket.getOutputStream());
      try (Scanner in = new Scanner(socket.getInputStream())) {

        ircWriter.establishConnection(config.getProperty("twitchUsername"),
            config.getProperty("twitchOauthToken"));

        ircWriter.join(mChannelName);

        while (in.hasNext() && !Thread.interrupted()) {
          String serverMessage = in.nextLine();
          // We only want to route server messages that represent users' chat messages.
          // We're also checking whether the message we received from the server was a PING and if so
          // we send PONG response in order to keep the connection alive.
          if (!ircWriter.pong(serverMessage) && serverMessage.contains("PRIVMSG")) {
            mDataSender.sendData(serverMessage);
          }
        }
      }
    } catch (UnknownHostException e) {
      throw new AssertionError("This should never happen, it means that there is an error in"
          + " configuration and we can't recover.", e);
    } catch (IOException e) {
      Logger.getLogger(TwitchStreamsApiWrapper.class.getName()).warning(e.getMessage());
    }
  }

}
