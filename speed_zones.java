import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.*;

public class speed_zones {
public static void main(String[] args) {
    	
    	String momentsPath = "/data/nba_movement_data/moments/";
    	String playersPath = "/data/nba_movement_data/players.csv";
        String eventsPath = "/data/nba_movement_data/events/";
        String outputPath = "./output/speed_zones";
        
        SparkSession spark = SparkSession.builder()
                .appName("Speed Zones")
                .getOrCreate();

        // Define window specifications for partitioning and ordering data
        WindowSpec w = Window.partitionBy("player_id", "game_id", "quarter").orderBy(desc("game_clock"));
        WindowSpec moving_average = Window.partitionBy("player_id", "game_id", "quarter").orderBy(desc("game_clock")).rowsBetween(-5, 4);
        //EVENTNUM to take into account more than one substitution for a player in the same game and quarter
        WindowSpec w1 = Window.partitionBy("g_id", "PERIOD", "PLAYER1_ID","EVENTNUM") 
                .orderBy(abs(col("game_clock").minus(col("clock_seconds"))));

        // Read the moments data, filter out rows for the ball (-1), and cast columns to double
        Dataset<Row> player_zones = spark.read()
                .option("header", "true")
                .csv(momentsPath)
                .filter(col("player_id").notEqual(-1))
                .withColumn("x_loc", col("x_loc").cast("double"))
                .withColumn("y_loc", col("y_loc").cast("double"))
                .withColumn("game_clock", col("game_clock").cast("double"))
                // Drop duplicate rows based on game_id, player_id, quarter, position and game_clock - a lot of moments are duplicated
                .dropDuplicates(new String[]{"game_id", "player_id", "quarter", "x_loc", "y_loc", "game_clock"})
                // Calculate differences in x and y locations using window specification
                .withColumn("dx", col("x_loc").minus(lag(col("x_loc"), 1).over(w)))
                .withColumn("dy", col("y_loc").minus(lag(col("y_loc"), 1).over(w)))
                // Compute distance in meters 
                .withColumn("dist", sqrt(pow(col("dx"), 2).plus(pow(col("dy"), 2))).multiply(0.3048))
                // Calculate the speed in meters per second
                .withColumn("speed", col("dist").divide(0.04))
                // Filter out speeds greater than 12 m/s
                .filter(col("speed").leq(12))
                // Calculate the 10 moving average of speed
                .withColumn("avg_speed", avg("speed").over(moving_average))
                // Assign slow, normal and fast zones based on average speed
                .withColumn("speed_zone", 
                        when(col("avg_speed").lt(2), "slow")
                        .when(col("avg_speed").geq(2).and(col("avg_speed").lt(8)), "normal")
                        .otherwise("fast"));

        // Get all the substitutions
        Dataset<Row> substitutions = spark.read()
                .option("header", "true")
                .csv(eventsPath)
                .filter(col("EVENTMSGTYPE").equalTo("8"))
                .withColumn("clock_seconds",
                        split(col("PCTIMESTRING"), ":").getItem(0).cast("double").multiply(60)
                        .plus(split(col("PCTIMESTRING"), ":").getItem(1).cast("double"))) // convert to seconds
                .withColumnRenamed("GAME_ID", "g_id");        
        
        //We need to get the moments that correspond to a substitution
        Dataset<Row> player_zones_substitutions = player_zones.join(substitutions, 
                substitutions.col("g_id").equalTo(player_zones.col("game_id"))
                .and(substitutions.col("PERIOD").equalTo(player_zones.col("quarter")))
                .and(substitutions.col("PLAYER1_ID").equalTo(player_zones.col("player_id"))))
                // For each game, period an player, order the moments by the difference in time between the substitution and the moment 
                // so the closest one can be selected as a substitution
                .withColumn("rank", row_number().over(w1))
                // The moment that is closer to the substitution event for a player, game and quarter, is marked as substitution, the rest are not
                .withColumn("substitution", when(col("rank").equalTo(1), 1).otherwise(0))
                .filter(col("substitution").equalTo(1))
                // Avoid column ambiguity 
                .withColumnRenamed("game_id", "game_id_duplicate")
                .withColumnRenamed("player_id", "player_id_duplicate")
                .withColumnRenamed("quarter", "quarter_duplicate")
                .withColumnRenamed("speed_zone", "speed_zone_2")
                .withColumnRenamed("game_clock", "game_clock_2")
                .withColumnRenamed("dist", "dist_2");

        
        // Finally get the limiters for the speed zones
        Dataset<Row> player_zones_limited = player_zones.join(player_zones_substitutions,
                player_zones_substitutions.col("g_id").equalTo(player_zones.col("game_id"))
                .and(player_zones_substitutions.col("PERIOD").equalTo(player_zones.col("quarter")))
                .and(player_zones_substitutions.col("PLAYER1_ID").equalTo(player_zones.col("player_id"))) 
                .and(player_zones_substitutions.col("x_loc").equalTo(player_zones.col("x_loc")))
                .and(player_zones_substitutions.col("y_loc").equalTo(player_zones.col("y_loc")))
                .and(player_zones_substitutions.col("game_clock_2").equalTo(player_zones.col("game_clock"))),"left")
                .withColumn("substitution", when(col("substitution").isNull(), 0).otherwise(col("substitution")))

                // Is a new run always that it is a substitution, and if not, when the previous moment speed_zone is different (for that game, quarter and player)
                .withColumn("is_new", when(
                                col("substitution").equalTo(1)
                                .or(lag(col("speed_zone"), 1).over(w).isNull())
                                .or(col("speed_zone").notEqual(lag(col("speed_zone"), 1).over(w))),1).otherwise(0))
                                                       
                // Group by player_id and speed_zone, and aggregate the number of runs and total distance
                .groupBy("player_id", "speed_zone")
                .agg(sum("is_new").alias("num_runs"), round(sum("dist"), 2).alias("total_distance"));
        
        // Get all player_ids, for players that have at least played any game
        //Dataset<Row> players_id = player_zones_limited
        //      .select(col("player_id").alias("p_id")).distinct();

        Dataset<Row> players_id = spark.read()
                .option("header", "true")
                .csv(playersPath)
                .filter(col("SEASON").equalTo("2015"))// interested in players of 2015/16 season
                .select(col("PLAYER_ID").alias("p_id")).distinct();

        // Get the different speed zones (normal, slow, fast)
        Dataset<Row> zones = player_zones_limited.select(col("speed_zone").alias("s_zone")).distinct();

        // Create a cross join to get all combinations of players and speed zones
        Dataset<Row> zones_player_combinations = players_id.crossJoin(zones);
        
        // Perform a left join with the result to ensure all player-zone combinations are included
        Dataset<Row> result_with_all_zones = zones_player_combinations.join(player_zones_limited, 
                        zones_player_combinations.col("p_id").equalTo(player_zones_limited.col("player_id"))
                .and(zones_player_combinations.col("s_zone").equalTo(player_zones_limited.col("speed_zone"))), "left")
                      // Replace null values in total_distance with 0.0
                      .withColumn("total_distance", when(col("total_distance").isNull(), 0).otherwise(col("total_distance")))
                      // Replace null values in num_runs with 0
                      .withColumn("num_runs", when(col("num_runs").isNull(), 0).otherwise(col("num_runs")));
        
        result_with_all_zones.select(col("p_id").alias("player_id"), col("s_zone").alias("speed_zone"), col("num_runs"), col("total_distance"))
        .coalesce(1)
        .write()
        .mode("overwrite")
        .option("delimiter", " ")
        .option("header", "true")
        .csv(outputPath);

        spark.stop();
    }
}

