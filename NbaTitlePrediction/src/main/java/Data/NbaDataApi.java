package Data;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.json.JSONArray;
import org.json.JSONObject;



public class NbaDataApi {
    public static String getURL = "https://api.sportsdata.io/v3/nba/scores/json/Standings/2023?key=b8b46e62b7da4d56b74d915e13a92b63";

    public static List<String> CollectData() throws Exception {
        URL url = new URL(getURL);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        JSONArray standings = new JSONArray(response.toString());
        List<String> collectedData = new ArrayList<>();
        for (int i = 0; i < standings.length(); i++) {
            JSONObject standing = standings.getJSONObject(i);
            StringJoiner stringJoiner = new StringJoiner("\t");
            stringJoiner.add(Integer.toString(standing.getInt("TeamID")));
            stringJoiner.add(Integer.toString(standing.getInt("Season")));
            stringJoiner.add(Integer.toString(standing.getInt("SeasonType")));
            stringJoiner.add(standing.getString("Key"));
            stringJoiner.add(standing.getString("City"));
            stringJoiner.add(standing.getString("Name"));
            stringJoiner.add(standing.getString("Conference"));
            stringJoiner.add(Integer.toString(standing.getInt("ConferenceRank")));
            stringJoiner.add(Integer.toString(standing.getInt("Wins")));
            stringJoiner.add(Integer.toString(standing.getInt("Losses")));
            collectedData.add(stringJoiner.toString());
        }
        return collectedData;
    }
}
