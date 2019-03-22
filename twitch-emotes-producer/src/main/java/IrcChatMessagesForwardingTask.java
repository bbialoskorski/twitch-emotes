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
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 * Connects to irc channel and forwards chat messages to a consumer.
 */
public class IrcChatMessagesForwardingTask implements Runnable {

  private final String mIrcServerAddress;
  private final Integer mPort;
  private final String mDaemon;
  private final String mUsername;
  private final String mOAuthToken;
  private final String mChannelName;
  private final DataSender<String> mDataSender;

  /**
   * @param ircServerAddress address of irc server
   * @param port port to connect to
   * @param daemon daemon which will receive PONG messages
   * @param username irc username
   * @param oAuthToken irc OAuth token
   * @param channelName name of irc channel you want to forward messages from
   * @param dataSender forwarding chat messages to some consumer
   */
  IrcChatMessagesForwardingTask(String ircServerAddress, Integer port, String daemon,
      String username, String oAuthToken, String channelName, DataSender<String> dataSender) {
    mIrcServerAddress = ircServerAddress;
    mPort = port;
    mDaemon = daemon;
    mUsername = username;
    mOAuthToken = oAuthToken;
    mChannelName = channelName;
    mDataSender = dataSender;
  }

  @Override
  public void run() {
    try (Socket socket = new Socket(mIrcServerAddress, mPort)) {

      IrcWriterProxy ircWriter = new IrcWriterProxy(socket.getOutputStream(), mDaemon);
      try (Scanner in = new Scanner(socket.getInputStream())) {

        ircWriter.establishConnection(mUsername, mOAuthToken);

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
