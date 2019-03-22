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

import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * Handles sending messages to the irc server.
 */
public class IrcWriterProxy {

  private final PrintWriter mOut;
  private final String mDaemon;

  /**
   * @param outputStream output stream of irc server connection
   * @param daemon which should receive PONG messages
   */
  IrcWriterProxy(OutputStream outputStream, String daemon) {
    mOut = new PrintWriter(outputStream);
    mDaemon = daemon;
  }

  /**
   * Establishes connection with irc server.
   *
   * @param username irc username
   * @param oAuthToken OAuth token to irc server
   */
  public void establishConnection(String username, String oAuthToken) {
    write("PASS", oAuthToken);
    write("NICK", username);
  }

  /**
   * Joins the IRC of specified channel.
   *
   * @param channelName name of IRC channel you want to join
   */
  public void join(String channelName) {
    String formattedChannelName = channelName.toLowerCase().replace(" ", "");
    write("JOIN", "#" + formattedChannelName);
  }

  /**
   * Leaves the IRC channel
   *
   * @param channelName name of IRC channel you want to leave.
   */
  public void part(String channelName) {
    String formattedChannelName = channelName.toLowerCase().replace(" ", "");
    write("PART", "#" + formattedChannelName);
  }

  /**
   * Checks whether server message was a PING and if it was sends PONG response.
   *
   * @param serverMessage message received from the server
   */
  public boolean pong(String serverMessage) {
    if (serverMessage.matches("PING " + mDaemon)) {
      write("PONG", mDaemon);
      return true;
    }
    return false;
  }

  private void write(String command, String message) {
    String fullMessage = command + " " + message + "\r\n";
    mOut.print(fullMessage);
    mOut.flush();
  }

}
