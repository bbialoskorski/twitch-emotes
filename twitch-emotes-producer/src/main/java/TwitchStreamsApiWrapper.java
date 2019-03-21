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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import javafx.util.Pair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * Wraps around twitch.tv Streams api.
 *
 * @see <a href="https://dev.twitch.tv/docs/api/reference/#get-streams"> Api reference </a>
 */
public class TwitchStreamsApiWrapper {

  private Properties mConfig;

  TwitchStreamsApiWrapper () {
    mConfig = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream stream = loader.getResourceAsStream("config.properties");
    try {
      mConfig.load(stream);
    } catch (IOException e) {
      // Invalid config file, we can't recover from this.
      throw new AssertionError("Invalid config.properties file, can't recover from this.", e);
    }
  }

  /**
   * Fetches a set of names of maximum _count_ top live streams in terms of viewer count.
   *
   * @param count maximum amount of streams you want to fetch
   * @return HashSet containing stream names of maximum _count_ streams.
   * @throws IOException in case of a problem with execution of http request
   */
  public HashSet<String> getLiveStreamNames(int count) throws IOException {
    HashSet<String> streamNames;
    // This guarantees that we won't fetch too many pages when count is divisible by 100.
    int pages = count % 100 == 0 ? (count / 100) - 1 : count / 100;

    // In the first api call we will fetch remainder of count / 101 streams and further calls will
    // fetch streams in batches of 100 because that is the maximum amount we can get in a single
    // response.
    int firstPageSize = count % 101;

    URIBuilder builder;
    ResponseHandler<Pair<String, ArrayList<String>>> resHandler = new StreamListResponseHandler();

    try {
      builder = new URIBuilder(mConfig.getProperty("twitchApiStreamsRoute"));
      // This parameter controls how many streams we will get in a response. This parameter has an
      // upper bound of 100 streams.
      builder.setParameter("first", Integer.toString(firstPageSize));
    } catch (URISyntaxException e) {
      throw new AssertionError("This should never happen, it means there is an error in "
          + "configuration and we cannot recover.", e);
    }

    HttpGet request;
    Pair<String, ArrayList<String>> processedResponseData = null;

    try {
      request = new HttpGet(builder.build());
      request.setHeader("Client-ID", mConfig.getProperty("twitchClientId"));
      // Loading first page of streams with (count mod 100) streams. Rest of streams will be loaded
      // in batches of 100.
    } catch (URISyntaxException e) {
      throw new AssertionError("This should never happen, it means that there is an error "
          + "in configuration and we can't recover.", e);
    }

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

      processedResponseData = httpClient.execute(request, resHandler);

      // Adding stream names from fetched page to streamNames HashSet and recovering pointer to the
      // end of fetched page.
      String nextPagePointer = processedResponseData.getKey();
      streamNames = new HashSet<>(processedResponseData.getValue());

      // Remaining streams (if any) will be loaded in batches of 100.
      builder.setParameter("first", "100");

      // Loading further pages of streams.
      for (int i = 0; i < pages; i++) {
        // This parameter controls from which page we want to get our results. Keep in mind that as
        // viewer count changes order of streams on the list can change in between requests and
        // thus contents of the page we're requesting might contain duplicates with our previous
        // request. This is acceptable and should occur very sporadically.
        builder.setParameter("after", nextPagePointer);

        try {
          request = new HttpGet(builder.build());
          request.setHeader("Client-ID", mConfig.getProperty("twitchClientId"));
          processedResponseData = httpClient.execute(request, resHandler);
          nextPagePointer = processedResponseData.getKey();
          streamNames.addAll(processedResponseData.getValue());
        } catch (URISyntaxException | ClientProtocolException e) {
          throw new AssertionError("This should never happen, it means that there is an error "
              + "in configuration and we can't recover.", e);
        }
      }
    }

    return streamNames;
  }

}
