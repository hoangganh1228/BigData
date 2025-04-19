package com.project.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;
public class Cau3 {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
                .appName("Spark CSV Example")
                .master("local")  // Chạy trên local
                .getOrCreate();
		String seedsource1Path = "C:/Work Space/Java/spark-project/src/main/resources/seedsource1.csv";
        String treeSpecPath = "C:/Work Space/Java/spark-project/src/main/resources/TreeSpec.csv";
        
        Dataset<Row> seedsource1DF = spark.read().option("header", "true").csv(seedsource1Path);   // Doc file vao data frame
        Dataset<Row> treespecDF = spark.read().option("header", "true").csv(treeSpecPath);
        
        // Hiển thị schema của các DataFrame
//        seedsource1DF.printSchema();
//        treespecDF.printSchema();
        
//        3.2
//        long acceptCodeCount = seedsource1DF.select("AcceptedCode").filter(not(col("AcceptedCode").equalTo(""))).distinct().count();
//        long treeSpecCodeCount = treespecDF.select("TreeSpecCode").filter(not(col("AcceptedCode").equalTo(""))).distinct().count();
////
//        System.out.println("Số lượng AcceptedCode: " + acceptCodeCount);
//        System.out.println("Số lượng TreeSpecCode: " + treeSpecCodeCount);
//        
//        3.3
//        Dataset<Row> eucalyptusDF = treespecDF.filter(treespecDF.col("TreeSpecLatin").like("Eucalyptus%"));
//        Dataset<Row> eucalyptusAcceptCodes = seedsource1DF.join(eucalyptusDF, "TreeSpecCode");
//        eucalyptusAcceptCodes.select("AcceptedCode").distinct().show();

        // Hiển thị dữ liệu từ cả hai DataFrame
//        seedsource1DF.show();
//        treespecDF.show();
        	
//        3.4
//        Dataset<Row> top10Area = seedsource1DF
//        		.withColumn("AreaDouble", col("Area").cast("double"))
//        	    .orderBy(col("AreaDouble").desc())
//        	    .limit(10);
//        top10Area.show();
        
//        3.5
//        long countAcceptedCodeGreaterThan3 = seedsource1DF
//        	    .withColumn("AreaDouble", col("Area").cast("double")) // Ép kiểu
//        	    .filter(col("AreaDouble").gt(3))                      // Lọc > 3
//        	    .select("AcceptedCode")
//        	    .distinct()
//        	    .count();
//
//        	System.out.println("Số lượng AcceptedCode có diện tích > 3: " + countAcceptedCodeGreaterThan3);
        
//        3.6
//        Dataset<Row> communeCount = seedsource1DF
//        	    // Lọc bỏ giá trị null hoặc chuỗi rỗng / khoảng trắng
//        	    .filter(col("ProvinceCode").isNotNull()
//        	        .and(trim(col("ProvinceCode")).notEqual("")))
//        	    
//        	    // Ép kiểu ProvinceCode sang số nguyên (int)
//        	    .withColumn("ProvinceCodeInt", col("ProvinceCode").cast("int"))
//        	    
//        	    // Lọc lại lần nữa nếu sau khi ép kiểu mà thành null (do ép thất bại)
//        	    .filter(col("ProvinceCodeInt").isNotNull())
//        	    
//        	    // Group by và đếm số xã duy nhất
//        	    .groupBy("ProvinceCodeInt")
//        	    .agg(countDistinct("CommuneCode").alias("CommuneCount"));
//
//    	communeCount.show();
//        
//        3.7
// Chuyển cột Area thành kiểu Double để tính toán
	//        Dataset<Row> seedsource1WithArea = seedsource1DF.withColumn("Area", 
	//                functions.col("Area").cast(DataTypes.DoubleType));
	//        Dataset<Row> avgAreaPerCommune = seedsource1WithArea.groupBy("CommuneCode").avg("Area");
	//        avgAreaPerCommune.show();
        
//        3.8
//        Dataset<Row> withAreaMultiplied = seedsource1DF.withColumn("AreaTimes10", seedsource1DF.col("Area").multiply(10));
//        withAreaMultiplied.show();
        
//        3.9
//        Dataset<Row> newColumn = seedsource1DF.withColumn("NewAcceptedCode", 
//        	    functions.when(seedsource1DF.col("AcceptedCode").startsWith("SD"), "SD")
//        	    .when(seedsource1DF.col("AcceptedCode").startsWith("SM"), "SM")
//        	    .when(seedsource1DF.col("AcceptedCode").startsWith("SC"), "SC")
//        	    .otherwise("Other"));
//        	newColumn.show();
        
//        3.10
//        Dataset<Row> areaCategory = seedsource1DF.withColumn("AreaCategory",
//        	    functions.when(seedsource1DF.col("Area").leq(1), "small")
//        	    .when(seedsource1DF.col("Area").gt(1).and(seedsource1DF.col("Area").lt(3)), "Normal")
//        	    .otherwise("large"));
//        	areaCategory.show();



        	

        // Dừng Spark session khi xong
        spark.stop();
        
        
	}
}
