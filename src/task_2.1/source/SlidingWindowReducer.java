import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SlidingWindowReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
    private TreeMap<Date, Map<String, Double>> windowData = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        String[] parts = key.toString().split(",");
        String dateStr = parts[0];
        String category = parts[1];
        double amount = 0;
        for (DoubleWritable val : values) {
            amount += val.get();
        }

        try {
            Date date = dateFormat.parse(dateStr);
            Map<String, Double> categoryMap = windowData.getOrDefault(date, new HashMap<>());
            categoryMap.put(category, amount);
            windowData.put(date, categoryMap);
        } catch (ParseException e) {
            // Bỏ qua lỗi
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (windowData.isEmpty()) return;
        context.write(new Text("report_date\tcategory\trevenue"), null);
        
        Date lastDate = windowData.lastKey();
        Calendar cal = Calendar.getInstance();

        Date firstDate = windowData.firstKey();
        cal.setTime(firstDate);
        Date endDate = addDays(lastDate, 2);

        while (!cal.getTime().after(endDate)) {
            Date currentDate = cal.getTime();
            emitWindowData(context, currentDate);
            cal.add(Calendar.DATE, 1);
        }
    }

    private Date addDays(Date date, int days) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, days);
        return cal.getTime();
    }

    private void emitWindowData(Context context, Date reportDate)
            throws IOException, InterruptedException {
        Map<String, Double> totalByCategory = new HashMap<>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(reportDate);
        cal.add(Calendar.DATE, -2); // Bắt đầu từ ngày trước đó 2 ngày
        Date startDate = cal.getTime();
        
        // Tính tổng cho 3 ngày (ngày hiện tại và 2 ngày trước)
        for (int i = 0; i < 3; i++) {
            Map<String, Double> dailyData = windowData.getOrDefault(startDate, new HashMap<>());
            for (Map.Entry<String, Double> entry : dailyData.entrySet()) {
                String cat = entry.getKey();
                double val = entry.getValue();
                totalByCategory.put(cat, totalByCategory.getOrDefault(cat, 0.0) + val);
            }
            cal.add(Calendar.DATE, 1);
            startDate = cal.getTime();
        }

        List<Map.Entry<String, Double>> sortedCategories = new ArrayList<>(totalByCategory.entrySet());
        Collections.sort(sortedCategories, Map.Entry.comparingByKey());

        String reportDateStr = dateFormat.format(reportDate);
        for (Map.Entry<String, Double> entry : sortedCategories) {
            String output = String.format("%.1f", entry.getValue());
            String formattedOutput = reportDateStr + "\t" + entry.getKey() + "\t" + output;
            context.write(new Text(formattedOutput), null);
        }
    }
}