package fn;

import model.WatchLog;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.JSONObject;


public class ParseJson extends DoFn<String, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {

        String payload = c.element();
        JSONObject obj = new JSONObject(payload);
        WatchLog watchLogObj = new WatchLog(obj);
        KV objectKV = KV.of(watchLogObj.getMovieId(), watchLogObj.toString());
        c.output(objectKV);
    }
}
