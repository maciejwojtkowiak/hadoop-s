package hotels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Reducer for hotel reservations statistics.
 * Input key: hotel_id\tyear
 * Input value: count,sum_price,cancelled_count (from combiner)
 *           or price,is_cancelled (from mapper, if no combiner)
 *
 * Output: hotel_id\tyear\treservation_count\tavg_price\tcancelled_count
 */
public class ReservationReducer extends Reducer<Text, Text, Text, Text> {

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
                // From mapper directly: price,is_cancelled
                sumPrice += Double.parseDouble(parts[0]);
                cancelled += Integer.parseInt(parts[1]);
                count++;
            } else if (parts.length == 3) {
                // From combiner: count,sum_price,cancelled_count
                count += Long.parseLong(parts[0]);
                sumPrice += Double.parseDouble(parts[1]);
                cancelled += Long.parseLong(parts[2]);
            }
        }

        // Calculate average price with 2 decimal places
        BigDecimal avg = BigDecimal.valueOf(sumPrice)
                .divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP);

        String[] keyParts = key.toString().split("\t");
        String hotelId = keyParts[0];
        String year = keyParts[1];

        outValue.set(count + "\t" + avg.toPlainString() + "\t" + cancelled);
        context.write(new Text(hotelId + "\t" + year), outValue);
    }
}
