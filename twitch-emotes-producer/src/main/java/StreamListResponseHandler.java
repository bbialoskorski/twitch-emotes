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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.ArrayList;
import javafx.util.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

/**
 * Handler processing twitch.tv streams api request's response into pair consisted of a String
 * which is a cursor to the end of fetched page and ArrayList of fetched stream names.
 *
 * @see <a href="https://dev.twitch.tv/docs/api/reference/#get-streams"> Api reference </a>
 */
public class StreamListResponseHandler implements ResponseHandler<Pair<String, ArrayList<String>>> {

  @Override
  public Pair<String, ArrayList<String>> handleResponse(HttpResponse response) throws IOException {
    ArrayList<String> streamNames = new ArrayList<>();
    String jsonString;
    String cursor; // Pointer to the end of response page for further requests.

    HttpEntity entity = response.getEntity();

    jsonString = EntityUtils.toString(entity);

    JsonObject responseJson = new JsonParser().parse(jsonString).getAsJsonObject();

    JsonArray data = responseJson.getAsJsonArray("data");

    cursor = ((JsonObject) responseJson.get("pagination")).get("cursor").getAsString();

    for (JsonElement element : data) {
      streamNames.add(((JsonObject) element).get("user_name").getAsString());
    }

    return new Pair<>(cursor, streamNames);
  }
}
