# 1. Biên dịch mã Java vào thư mục build/
javac -classpath $(hadoop classpath) -d build/ SlidingWindowMapper.java SlidingWindowReducer.java SlidingWindowRevenue.java

# 2. Đóng gói thành file JAR và lưu vào thư mục jar/

 jar -cvf jar/SlidingWindowRevenue.jar -C build/ .

# 3. Xóa output cũ trên HDFS nếu có
hadoop fs -rm -r /user/youruser/output_slidingwindow

# 4. Chạy chương trình Hadoop
hadoop jar jar/SlidingWindowRevenue.jar SlidingWindowRevenue /user/youruser/asr.csv /user/youruser/output_slidingwindow

# 5. Tải kết quả về máy local
hadoop fs -get /user/youruser/output_slidingwindow/part-r-00000 output/result_slidingwindow.csv
