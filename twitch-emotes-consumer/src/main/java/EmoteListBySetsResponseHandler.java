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
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

/**
 * Handler processing twitch.tv api call for list of emotes for specified emote sets response into
 * unified ArrayList of emotes.
 */
public class EmoteListBySetsResponseHandler
    implements ResponseHandler<ArrayList<String>> {

  private ArrayList<String> mEmoteSets;

  /**
   * @param emoteSets list of emote sets numbers for which you want to get emote
   */
  EmoteListBySetsResponseHandler(ArrayList<String> emoteSets) {
    mEmoteSets = emoteSets;
  }

  @Override
  public ArrayList<String> handleResponse(HttpResponse response) throws IOException {
    ArrayList<String> emotes = new ArrayList<>();
    String jsonString;

    HttpEntity entity = response.getEntity();
    jsonString = EntityUtils.toString(entity);

    JsonObject responseJson = new JsonParser().parse(jsonString).getAsJsonObject();

    JsonObject emoteSetsJson = responseJson.getAsJsonObject("emoticon_sets");

    for (String emoteSet : mEmoteSets) {
      JsonArray emotesJsonArray = ((JsonObject)emoteSetsJson).getAsJsonArray(emoteSet);

      for (JsonElement emote : emotesJsonArray) {
        String emoteString = ((JsonObject)emote).get("code").getAsString();
        emotes.add(emoteString);
      }
    }

    return emotes;
  }
}
