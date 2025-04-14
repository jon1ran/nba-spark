import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.*;

public class rebounds {
public static void main(String[] args) {
    	
    	String momentsPath = "/data/nba_movement_data/moments/";
        String eventsPath = "/data/nba_movement_data/events/";
        String outputPath = "./output/rebounds";
        
        SparkSession spark = SparkSession.builder()
                .appName("Rebounds")
                .getOrCreate();

        // Read the events data, filter for rebounds (EVENTMSGTYPE = 4) and remove team rebounds
        Dataset<Row> rebounds = spark.read()
                .option("header", "true")
                .csv(eventsPath)
                .filter(col("EVENTMSGTYPE").equalTo("4"))
                .filter(col("PLAYER1_NAME").isNotNull()) // remove team rebounds
                .withColumn("clock_seconds",
                        split(col("PCTIMESTRING"), ":").getItem(0).cast("double").multiply(60)
                        .plus(split(col("PCTIMESTRING"), ":").getItem(1).cast("double"))); // convert to seconds

        // Read the moments data, filter for shot clock equals 24.0, because we are interested in the moment where the rebound happens
        Dataset<Row> moments = spark.read()
                .option("header", "true")
                .csv(momentsPath)
                .filter(col("player_id").notEqual(-1))
                .filter(col("shot_clock").equalTo("24.0"))
                .withColumn("x_loc", col("x_loc").cast("double"))
                .withColumn("y_loc", col("y_loc").cast("double"))
                .withColumn("game_clock", col("game_clock").cast("double"))
                .withColumn("shot_clock", col("shot_clock").cast("double"))
                .withColumnRenamed("game_id", "g_id")
                .dropDuplicates(new String[]{"g_id", "player_id", "quarter", "x_loc", "y_loc", "game_clock"}); // drop duplicate moments
        
        // Define a window specification to partition by game, period, and player, and order by the absolute difference between game clock and clock seconds
        WindowSpec w1 = Window.partitionBy("GAME_ID", "PERIOD", "PLAYER1_ID", "EVENTNUM")
                .orderBy(abs(col("game_clock").minus(col("clock_seconds"))));

        // Join rebounds and moments datasets and find the closest moment to the rebound
        Dataset<Row> rebounds_with_max_distance = rebounds.join(moments, 
                rebounds.col("GAME_ID").equalTo(moments.col("g_id"))
                .and(rebounds.col("PERIOD").equalTo(moments.col("quarter")))
                .and(rebounds.col("PLAYER1_ID").equalTo(moments.col("player_id"))), "left")
                // For each game, period an player, order the moments by the difference in time
                .withColumn("rank", row_number().over(w1))
                // Select the closest moment to the rebound, the first row after ordering based on the time difference
                .filter(col("rank").equalTo(1))
                // Calculate the distance between the rebound/moment location and the baskets, and get the minimum, since it the basket where
                // the robound happens
                .withColumn("distance1", sqrt(pow(col("x_loc").minus(6), 2).plus(pow(col("y_loc").minus(25), 2))))
                .withColumn("distance2", sqrt(pow(col("x_loc").minus(89), 2).plus(pow(col("y_loc").minus(25), 2))))
                .withColumn("distance", least(col("distance1"), col("distance2")).multiply(0.3048)) // convert feet to meters
                // Get the descriptions (one of them is empty) and extract the offensive and defensive rebounds
                .withColumn("joint", coalesce(col("VISITORDESCRIPTION"), col("HOMEDESCRIPTION")))
                .withColumn("off", regexp_extract(col("joint"), ".*Off:(\\d+).*", 1))
                .withColumn("def", regexp_extract(col("joint"), ".*Def:(\\d+).*", 1))
                // Group by player and game, and aggregate the maximum offensive and defensive rebounds (this is the values at the end of the game)
                .groupBy("PLAYER1_ID", "GAME_ID")
                .agg(max("off").alias("maxoff"), max("def").alias("maxdef"), max("distance").alias("distance"))
                // Finally, for each player calculate the total offensive and defensive rebounds, and the farthest rebound
                .groupBy("PLAYER1_ID")
                .agg(sum("maxoff").cast("int").alias("offensive_count"), sum("maxdef").cast("int").alias("defensive_count"),
                        round(max("distance"), 2).alias("dist_farthest_rebound"));

        rebounds_with_max_distance.select(col("PLAYER1_ID").alias("player_id"), col("offensive_count"), col("defensive_count"), col("dist_farthest_rebound"))
                .coalesce(1)
                .write()
                .mode("overwrite")
                .option("delimiter", " ")
                .option("header", "true")
                .csv(outputPath);
                    
        spark.stop();
    }
}
