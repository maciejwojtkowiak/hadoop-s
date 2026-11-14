package hotels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Combiner for hotel reservations statistics.
 * Aggregates partial sums: count, sum_price, cancelled_count.
 * Output value format: count,sum_price,cancelled_count
 *
 * Handles both mapper output (price,is_cancelled) and combiner output
 * (count,sum_price,cancelled_count) transparently.
 */
public class ReservationCombiner extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long count = 0;
        double sumPrice = 0.0;
        long cancelled = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length == 2) {
                // From mapper: price,is_cancelled
                sumPrice += Double.parseDouble(parts[0]);
                cancelled += Integer.parseInt(parts[1]);
                count++;
            } else if (parts.length == 3) {
                // From another combiner: count,sum_price,cancelled_count
                count += Long.parseLong(parts[0]);
                sumPrice += Double.parseDouble(parts[1]);
                cancelled += Long.parseLong(parts[2]);
            }
        }

        outValue.set(count + "," + sumPrice + "," + cancelled);
        context.write(key, outValue);
    }
}
