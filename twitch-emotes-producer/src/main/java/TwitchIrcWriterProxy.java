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
public class TwitchIrcWriterProxy {

  private PrintWriter mOut;

  /**
   * @param outputStream output stream of irc server connection
   */
  TwitchIrcWriterProxy(OutputStream outputStream) {
    mOut = new PrintWriter(outputStream);
  }

  /**
   * Establishes connection with irc server.
   */
  public void establishConnection(String username, String oauthToken) {
    write("PASS", oauthToken);
    write("NICK", username);
  }

  /**
   * Joins the IRC of specified twitch.tv channel.
   *
   * @param channelName name of twitch.tv channel which chat you want to join.
   */
  public void join(String channelName) {
    String formattedChannelName = channelName.toLowerCase().replace(" ", "");
    write("JOIN", "#" + formattedChannelName);
  }

  /**
   * Leaves the IRC channel of specified twitch.tv channel.
   *
   * @param channelName name of twitch.tv channel which chat you want to leave.
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
    if (serverMessage.matches("PING :tmi.twitch.tv")) {
      write("PONG", ":tmi:twitch.tv");
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
