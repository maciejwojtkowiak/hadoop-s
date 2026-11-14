package hotels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Mapper for hotel reservations statistics.
 * Input: CSV lines from reservations (datasource1)
 * Output key: hotel_id\tyear
 * Output value: price,is_cancelled(0|1)
 *
 * Emits sum components (price, cancelled flag) so that a Combiner can
 * aggregate partial results correctly (no pre-computed averages).
 */
public class ReservationMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Skip CSV header
        if (line.startsWith("reservation_id")) return;

        String[] parts = line.split(",");
        if (parts.length < 7) return;

        try {
            String hotelId = parts[1].trim();
            String checkIn = parts[3].trim();
            String priceStr = parts[5].trim();
            String status  = parts[6].trim();

            // Extract year from check_in (yyyy-MM-dd'T'HH:mm)
            String year = checkIn.substring(0, 4);
            Integer.parseInt(year); // validate

            double price = Double.parseDouble(priceStr);
            int isCancelled = "Cancelled".equals(status) ? 1 : 0;

            outKey.set(hotelId + "\t" + year);
            outValue.set(price + "," + isCancelled);

            context.write(outKey, outValue);
        } catch (Exception e) {
            // skip malformed records
        }
    }
}
