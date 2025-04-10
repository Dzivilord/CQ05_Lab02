import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SlidingWindowMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final SimpleDateFormat inputFormat = new SimpleDateFormat("MM-dd-yy");
    private final SimpleDateFormat outputFormat = new SimpleDateFormat("dd/MM/yyyy");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("Order ID")) {
            return;
        }

        String[] fields = value.toString().split(",");
        try {
            String status = fields[3].trim();
            if (!status.contains("Shipped")) {
                return;
            }

            String dateStr = fields[2].trim();
            String category = fields[9].trim();
            String amountStr = fields[15].trim();

            Date date = inputFormat.parse(dateStr);
            String formattedDate = outputFormat.format(date);

            double amount = Double.parseDouble(amountStr);
            String outputKey = formattedDate + "," + category;

            context.write(new Text(outputKey), new DoubleWritable(amount));
        } catch (ParseException | NumberFormatException | ArrayIndexOutOfBoundsException e) {
            // Bỏ qua lỗi
        }
    }
}