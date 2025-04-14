# NBA Big Data Analytics - Spark on Hadoop

## Overview

This project is part of a distributed computing and big data processing assignment, where we leveraged Apache Spark on a Hadoop cluster to process and analyze large-scale NBA movement tracking data. Each game tracked millions of positional data points sampled at 25Hz for all players and the ball, resulting in millions of records across the season.

Due to the massive size of the dataset and the high frequency of the spatial data, we utilized **Hadoop** for distributed storage and **Spark SQL** for distributed computation. This ensured efficient data synchronization, cleaning, and parallel processing across the cluster.

## Features

The project comprises three main analytical modules, implemented in Java using Apache Spark:

### 1. 🏃 Distance per Player
- Computes the total distance each player traveled during the season.
- Normalizes the distance by minutes played to report **average distance per quarter**.
- Handles **duplicate moments** and ensures **data consistency** across games.

> Output: `distance_per_player.csv`  
> Format: `player_id avg_distance_per_quarter`

### 2. ⚡ Speed Zones Analysis
- Categorizes player movements into three **speed zones**: `slow`, `normal`, and `fast`.
- Applies a **moving average filter** over 10-frame windows to smoothen noise and eliminate unrealistic spikes (e.g., >12 m/s).
- Identifies **"runs"** within consistent speed zones, resetting on zone change, substitution, or quarter end.
- Aggregates the number of runs and total distance per zone.

> Output: `speed_zones.csv`  
> Format: `player_id speed_zone num_runs total_distance`

### 3. 🏀 Rebounds Analysis
- Detects **offensive** and **defensive** rebounds using play-by-play (PBP) data.
- Calculates the **farthest rebound distance** retrieved by each player.
- Synchronizes events with moments via timestamp alignment, addressing timing mismatches.

> Output: `rebounds.csv`  
> Format: `player_id nb_offensive_rebounds nb_defensive_rebounds dist_farthest_rebound`

## Why Big Data?

- Each game includes **millions of moments**, with positional data for 10 players and the ball sampled 25 times per second.
- Across **dozens of NBA games**, the data becomes infeasible to process on a single machine.
- Using Spark over Hadoop allowed scalable, distributed processing while ensuring **reliable ETL**, **efficient window operations**, and **robust analytics**.

## Technologies

- **Apache Spark (Java API)**
- **Hadoop Distributed File System (HDFS)**
- **Spark SQL**
- **DataFrames and Window Functions**
- **Unix Shell Scripts for Job Execution**



 
