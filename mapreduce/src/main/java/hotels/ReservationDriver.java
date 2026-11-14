package hotels;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver class for Hotel Reservation Statistics MapReduce job.
 *
 * Usage: hadoop jar hotel-reservations.jar hotels.ReservationDriver <input> <output>
 *
 * Groups reservations by (hotel_id, year) and computes:
 *   - reservation_count
 *   - avg_price
 *   - cancelled_count
 */
public class ReservationDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ReservationDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hotel Reservation Statistics");

        job.setJarByClass(ReservationDriver.class);
        job.setMapperClass(ReservationMapper.class);
        job.setCombinerClass(ReservationCombiner.class);
        job.setReducerClass(ReservationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
