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
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * Wraps around twitch.tv api emote list calls.
 */
public class TwitchEmotesApiWrapper {

  private String mClientId;

  /**
   * @param clientId twitch.tv dev client id
   */
  TwitchEmotesApiWrapper(String clientId) {
    mClientId = clientId;
  }

  /**
   * Performs api call for list of twitch.tv global emotes.
   *
   * @return list of global twitch.tv emotes
   * @throws IOException in case of a problem or connection error
   */
  public ArrayList<String> getGlobalEmotes() throws IOException {
    ArrayList<String> emoteList;

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet("https://api.twitch.tv/kraken/chat/emoticon_images?emotesets=0");
      // HttpGet request = new HttpGet("https://api.twitch.tv/kraken/chat/emoticon_images");
      request.setHeader("Client-ID", mClientId);
      request.setHeader("Accept", "application/vnd.twitchtv.v5+json");

      ArrayList<String> emoteSets = new ArrayList<>();
      emoteSets.add("0");

      emoteList = httpClient.execute(request, new EmoteListBySetsResponseHandler(emoteSets));
    }

    return emoteList;
  }

  /**
   * Performs api call for list of all twitch.tv emotes. This function returns a huge amount of
   * data and might take a long time to finish.
   *
   * @return list of all twitch.tv emotes
   * @throws IOException in case of a problem or connection error
   */
  public ArrayList<String> getAllEmotes() throws IOException {
    ArrayList<String> emoteList;

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet("https://api.twitch.tv/kraken/chat/emoticon_images");
      request.setHeader("Client-ID", mClientId);
      request.setHeader("Accept", "application/vnd.twitchtv.v5+json");

      emoteList = httpClient.execute(request, new AllEmotesListResponseHandler());
    }

    return emoteList;
  }

}
