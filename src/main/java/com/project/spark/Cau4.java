package com.project.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Cau4 {
	public static void main(String[] args) {
        // Khởi tạo SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Real Madrid Cards")
                .master("local") // Chạy trên local
                .getOrCreate();
        
     // Đọc dữ liệu từ tệp CSV trong thư mục resources
        String filePath = "C:/Work Space/Java/spark-project/src/main/resources/SP1_2017_2018.csv";  // Cập nhật đường dẫn tệp nếu cần
        Dataset<Row> laligaDF = spark.read().option("header", "true").csv(filePath);
        
     // Lọc các trận đấu có sự tham gia của Real Madrid
        Dataset<Row> realMadridMatches = laligaDF.filter(
                laligaDF.col("HomeTeam").equalTo("Real Madrid").or(laligaDF.col("AwayTeam").equalTo("Real Madrid"))
        );
        
     // Tính tổng số thẻ vàng và thẻ đỏ cho Real Madrid
        Dataset<Row> realMadridCards = realMadridMatches.withColumn("RealMadrid_YellowCards",
                functions.when(realMadridMatches.col("HomeTeam").equalTo("Real Madrid"), realMadridMatches.col("HY"))
                        .otherwise(realMadridMatches.col("AY"))
        ).withColumn("RealMadrid_RedCards",
                functions.when(realMadridMatches.col("HomeTeam").equalTo("Real Madrid"), realMadridMatches.col("HR"))
                        .otherwise(realMadridMatches.col("AR"))
        );
        
     // Tính tổng số thẻ vàng và thẻ đỏ
        double totalYellowCards = realMadridCards.agg(functions.sum("RealMadrid_YellowCards")).first().getDouble(0);
        double totalRedCards = realMadridCards.agg(functions.sum("RealMadrid_RedCards")).first().getDouble(0);

        // In kết quả
        System.out.println("Total Yellow Cards for Real Madrid: " + totalYellowCards);
        System.out.println("Total Red Cards for Real Madrid: " + totalRedCards);

        // Dừng Spark session khi xong
        spark.stop();
	}
}
