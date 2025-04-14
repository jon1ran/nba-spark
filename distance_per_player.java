import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.*;


public class distance_per_player {
    public static void main(String[] args) {
    	
    	String momentsPath = "/data/nba_movement_data/moments/";
        String minutesPath = "/data/nba_movement_data/minutes_played.csv";
        String outputPath = "./output/distance_per_player";
        
        SparkSession spark = SparkSession.builder()
                .appName("Distance Travelled")
                .getOrCreate();
        
        // Define window specification for calculating differences
        WindowSpec w = Window.partitionBy("player_id", "game_id", "quarter").orderBy(desc("game_clock"));

        // Read the moments data, filter out rows where player_id is -1, and cast required columns to double
        Dataset<Row> distance = spark.read()
                .option("header", "true")
                .csv(momentsPath)
                .filter(col("player_id").notEqual(-1))
                .withColumn("x_loc", col("x_loc").cast("double"))
                .withColumn("y_loc", col("y_loc").cast("double"))
                .withColumn("game_clock", col("game_clock").cast("double"))
                // Drop duplicate rows based on game_id, player_id, quarter,position  and game_clock - a lot of moments are duplicated
                .dropDuplicates(new String[]{"game_id", "player_id", "quarter", "x_loc", "y_loc", "game_clock"})
                // Calculate differences in x and y locations using window specification
                .withColumn("dx", col("x_loc").minus(lag(col("x_loc"), 1).over(w)))
                .withColumn("dy", col("y_loc").minus(lag(col("y_loc"), 1).over(w)))
                // Compute distance in meters 
                .withColumn("dist", sqrt(pow(col("dx"), 2).plus(pow(col("dy"), 2))).multiply(0.3048))
                // Aggregate total distance traveled by each player
                .groupBy("player_id")
                .agg(sum("dist").alias("total_distance"));

        // Read the seconds played data and aggregate total minutes played by each player
        Dataset<Row> minutes_player = spark.read()
                .option("header", "true")
                .csv(minutesPath)
                .groupBy("PLAYER_ID")
                .agg(sum("SEC").alias("total_seconds"))
                // Convert total seconds to total minutes
                .withColumn("total_minutes", col("total_seconds").divide(60))
                .withColumnRenamed("PLAYER_ID", "p_id");

        // Join the distance and minutes dataframes on player_id
        Dataset<Row> distance_minutes = distance.join(minutes_player, distance.col("player_id").equalTo(minutes_player.col("p_id")));

        // Calculate average distance traveled per quarter for each player
        Dataset<Row> result = distance_minutes
                .withColumn("avg_dist_quarter", 
                        round(col("total_distance").multiply(12).divide(col("total_minutes")), 0).cast("int"));

        result.select("player_id", "avg_dist_quarter")
        .coalesce(1)
        .write()
        .mode("overwrite")
        .option("delimiter", " ")
        .option("header", "true")
        .csv(outputPath);
        
        spark.stop();
    }
}
