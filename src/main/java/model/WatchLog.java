package model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;

import java.io.Serializable;

public class WatchLog {
    private String userId;
    private String movieId;
    private int position;
    private long timestamp;

    public WatchLog(JSONObject obj) {
        DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
        this.setUserId(obj.getString("userId"));
        this.setMovieId(obj.getString("movieId"));
        this.setPosition(obj.getInt("position"));
        this.setTimestamp(f.parseDateTime(obj.getString("timestamp")).getMillis());
    }

    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = null;
        try {
            jsonString = mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    public String getUserId() {
        return userId;
    }

    public String getMovieId() {
        return movieId;
    }

    public int getPosition() {
        return position;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}

