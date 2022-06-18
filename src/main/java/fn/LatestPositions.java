package fn;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.json.JSONObject;


public class LatestPositions extends DoFn<KV<String, Iterable<String>>, String> {
    @ProcessElement
    public void processElement(ProcessContext c)  throws Exception {
        Iterable<String> payload = c.element().getValue();
        long tsMemo = 0;
        String memo = null;
        for(String wl: payload){
            JSONObject obj = new JSONObject(wl);
            long ts = (long) obj.get("timestamp");
            if (ts >= tsMemo) {
                tsMemo = ts;
                memo = wl;
            }
        }
        c.output(memo);
    }
}
