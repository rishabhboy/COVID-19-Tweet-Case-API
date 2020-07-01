package utility;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Parser_json {

    public String filter(String line) throws ParseException {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(line);
        JSONObject jo = (JSONObject) obj;
        String s;
        JSONObject jo1 = (JSONObject) jo.get("retweeted_status");
        JSONObject jo2 = (JSONObject) jo.get("extended_tweet");
        if(jo1 == null && jo2 == null) s = (String) jo.get("text");
        else if (jo2 != null) s = (String) jo2.get("full_text");
        else {
            JSONObject jo3 = (JSONObject) jo1.get("extended_tweet");
            if (jo3 == null) s = (String) jo1.get("text");
            else s = (String) jo3.get("full_text");
        }
        JSONObject jo4 = (JSONObject) jo.get("user");
        if (jo4 == null) {
            return "-1";
        }
        String location = (String) jo4.get("location");
        String time = (String) jo.get("created_at");
        String[] temp = time.split(" ");
        s=s.replaceAll("[^a-zA-Z0-9]"," ");

        if(time == null || s == null || location == null){
            return "-1";
        }
        String [] location1=location.split(",");
        int len=location1.length;
        if(len == 0 || location1[len-1] == ""){
            return "-1";
        }
        String location2=location1[len-1];

        String day=temp[1];
        String month=temp[2];
        String country=location2.replaceAll("[^a-zA-Z0-9]","");

        String result = country + "," + day + "," + month + "," + s;
        System.out.println(result);
        return result;

    }
}
