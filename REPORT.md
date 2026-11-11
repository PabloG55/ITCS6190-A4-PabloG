# Assignment #4: Report

**Name:** Pablo Garces
**Student ID:** 801377963
**Email:** <pgarces@charlotte.edu>

---

## Seed and commands

Seed used for `datagen.py`: `801377963`

The commands you ran, in order:

```bash
python3 datagen.py 801377963

docker compose up -d

# The image does not come with numpy (same as in L11), so I installed it before running.
docker exec -u root spark-master pip install numpy
docker exec -u root spark-worker-1 pip install numpy
docker exec -u root spark-worker-2 pip install numpy

docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py part1a /opt/spark/work-dir/shared 801377963
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py part1b /opt/spark/work-dir/shared 801377963
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py part2 /opt/spark/work-dir/shared 801377963
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py part3 /opt/spark/work-dir/shared 801377963

python3 datagen.py 801377963 shared-folder /tmp/truth.csv
cat shared-folder/output/part1b/*.csv | grep -v ride_id | sort | head -5

docker compose down
```

---

## Part 1a: trips and revenue per zone per window

The micro-batch summary (batches, watermark, state rows):

```
=== part1a: micro-batch summary ===
batch input rows            watermark state rows
    0         45                    -         29
    1         41  2026-10-27 07:57:51         33
    2         43  2026-10-27 07:58:57         34
    3         43  2026-10-27 07:59:53         35
    4         36  2026-10-27 08:00:48         25
    5         39  2026-10-27 08:01:50         39
    6         37  2026-10-27 08:02:46         47
    7         36  2026-10-27 08:03:42         48
    8         41  2026-10-27 08:04:58         49
    9         45  2026-10-27 08:05:50         24
   10         44  2026-10-27 08:06:55         39
   11         45  2026-10-27 08:07:48         47
   12         42  2026-10-27 08:08:53         49
   13         38  2026-10-27 08:09:51         49
   14         42  2026-10-27 08:10:51         25
   15         41  2026-10-27 08:11:50         37
   16         37  2026-10-27 08:12:55         44
   17         42  2026-10-27 08:13:53         49
   18         43  2026-10-27 08:14:56         49
   19         38  2026-10-27 08:15:56         24
   20         36  2026-10-27 08:16:55         36
   21         40  2026-10-27 08:17:58         42
   22         35  2026-10-27 08:18:54         48
   23         36  2026-10-27 08:19:50         48
   24         37  2026-10-27 08:20:58         25
   25         41  2026-10-27 08:21:54         42
   26         45  2026-10-27 08:22:54         47
   27         39  2026-10-27 08:23:52         49
   28         45  2026-10-27 08:24:46         50
   29         40  2026-10-27 08:25:53         25
   30          0  2026-10-27 08:26:51         25
```

Which 5-minute windows did the stream write, and which are missing compared with the batch
table printed after the query? For one window that was written, compare its trips with the
batch value: where did the difference come from (look at how `datagen.py` makes the late
events)? Why does `state rows` drop every five batches?

The stream wrote six windows, from 07:55–08:00 to 08:20–08:25. The window 08:25–08:30 is
missing, because the last watermark is 08:26:51, which is before the end of that window, so
Spark never closed it and append mode never wrote it.

For the window 08:05–08:10, zone 1 has 20 trips in the stream and 21 in the batch table. In
`datagen.py`, from file 12 on, some rides are very late, with an `event_time` 8 to 10 minutes
before the file they are in. When that ride arrived, the watermark had already passed the end of
its window, so Spark threw it away. The rides that are only 1 to 2 minutes late are inside the
3-minute watermark, so they're kept.

`state rows` drops every five batches because each file is one minute of rides, so every five
batches the watermark passes the end of another 5-minute window. That window is written and
removed from the state, and only the window that's still open is kept.

## Part 1b: alerts

How many alerts, of which kind, and the first five lines of the output:

```
=== part1b: 16 alerts ===
+-------------+-----+
|       reason|count|
+-------------+-----+
|zero_distance|   16|
+-------------+-----+

$ cat shared-folder/output/part1b/*.csv | grep -v ride_id | sort | head -5
R00041,2026-10-27 08:00:07,5,0.0,19.1,36.24,zero_distance
R00090,2026-10-27 08:02:15,4,0.0,21.5,23.71,zero_distance
R00175,2026-10-27 08:04:24,14,0.0,17.1,19.78,zero_distance
R00349,2026-10-27 08:08:21,5,0.0,6.8,15.65,zero_distance
R00420,2026-10-27 08:09:35,3,0.0,13.5,13.74,zero_distance
```

There are 16 alerts and all of them are `zero_distance`. There are no `high_fare` alerts because
there isn't any ride in the stream that has a fare over 150.

This query has no watermark and no state. Why is that correct here, and what would change if
you also wanted "more than three alerts for the same driver in ten minutes"?

Part 1b checks each ride on its own and doesn't group or remember anything between batches, so
there isn't any state to clean up and nothing has to wait for it. A late ride is checked the same
way as any other.

To get that result, I'd group the alerts by `driver_id` and a 10-minute window and count
them. In this case, we'd have to keep the state, which would need a watermark so that the
state doesn't grow forever.

---

## Part 2: the fare model

The three lines printed by `evaluate()` (also in `shared-folder/output/part2/metrics`), the
"tuned" line with the best parameters, and the "best model" line:

```
seed 801377963: 47923 training rows, 12077 test rows
baseline   RMSE   3.80   R2  0.854
full       RMSE   1.72   R2  0.970
tuned: best maxDepth 6, numTrees 100
tuned      RMSE   2.53   R2  0.935
best model: full, saved to /opt/spark/work-dir/shared/models/fare
```

Your tuned model: which estimator, which parameters in the grid and why those, how many fits
the CrossValidator ran, and how long part 2 took. Did the tuning beat the full linear
pipeline? If not, what does that tell you about the data?

I used a `RandomForestRegressor` with the same grid as the slides: `maxDepth` 2, 4 and 6 and
`numTrees` 10 and 100. `maxDepth` controls how deep each tree can go and `numTrees` how many trees
are averaged. Part 2 took about 40 seconds on the cluster.

The tuning didn't beat the full linear pipeline: 2.53 against 1.72. This means the fare is
mostly linear, which means it grows with the distance and the duration, so a linear regression
fits it better than trees.

Why is `hour` turned into a string before `StringIndexer` (see `features_of`) instead of
being used as a number?

The fare doesn't go up steadily with the hour, only the rush hours are more expensive. As a
number, the model would have learned that later hours always cost more or less. But as a
category, each hour gets its own value, so the model can learn which hours are more expensive.

---

## Part 3: scoring the stream

The threshold used, the number of rides flagged, the ten rides with the largest deviation
and the per-window counts:

```
saved model: full, test RMSE 1.72; flagging rides with deviation > 6.88
=== part3: 61 rides flagged ===
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
|ride_id|event_time         |pickup_zone|distance_km|duration_min|payment_type|fare  |prediction|deviation|
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
|R01139 |2026-10-27 08:27:36|11         |10.96      |37.3        |card        |120.76|37.89     |82.87    |
|R00082 |2026-10-27 08:01:30|10         |10.77      |38.7        |app         |115.47|38.14     |77.33    |
|R00917 |2026-10-27 08:22:19|11         |9.63       |40.3        |card        |109.44|36.76     |72.68    |
|R01109 |2026-10-27 08:27:34|25         |11.76      |28.0        |cash        |101.89|36.83     |65.06    |
|R00454 |2026-10-27 08:11:20|11         |8.13       |28.7        |card        |93.69 |30.04     |63.65    |
|R01154 |2026-10-27 08:27:52|5          |9.28       |29.6        |card        |96.76 |41.33     |55.43    |
|R00215 |2026-10-27 08:05:11|5          |3.41       |13.5        |cash        |75.74 |26.64     |49.1     |
|R01012 |2026-10-27 08:25:03|14         |5.22       |20.2        |app         |70.27 |22.05     |48.22    |
|R00201 |2026-10-27 08:04:44|19         |5.04       |13.2        |card        |63.62 |19.28     |44.34    |
|R00413 |2026-10-27 08:10:00|6          |7.92       |30.4        |app         |73.4  |30.34     |43.06    |
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
only showing top 10 rows
=== part3: flagged rides per 5-minute window (batch over the output) ===
+-------------------+-----+
|       window_start|count|
+-------------------+-----+
|2026-10-27 08:00:00|   14|
|2026-10-27 08:05:00|   10|
|2026-10-27 08:10:00|    8|
|2026-10-27 08:15:00|    6|
|2026-10-27 08:20:00|   12|
|2026-10-27 08:25:00|   11|
+-------------------+-----+
```

Run `python3 datagen.py <seed> shared-folder /tmp/truth.csv` (the third argument writes the
list of planted anomalies, which the generator does not reveal otherwise) and compare it with
your flagged rides: how many of the planted anomalies did you catch, and how many flagged
rides were not planted? Look at the false positives: what do they have in common, and what
would you change in part 2 to reduce them?

I caught all 42 planted anomalies, but 19 of the 61 flagged rides were false positives because
they have a fare higher than the prediction, and they're either rides with zero distance or
very long rides. In part 2, I'd add a feature that combines the distance with rush hour,
because during rush hour long trips cost more than the model expects, so in that way the model
would know that. Also, I wouldn't score the zero-distance rides because part 1b already reports
them.

Why is there no `fit()` in part 3, and what would happen if you tried one on the stream?
Which stages of the loaded model are fitted models, and what did each one learn from the
training data?

`fit()` needs to see all the data to learn from it, and a stream never ends, so there's never a
complete dataset. If you try it, Spark gives an error saying that queries with streaming sources
must be executed with `writeStream.start()`.

The fitted models are the `StringIndexer`, which learned which number goes with each category of
`zone`, `payment_type` and `hour`, and the `OneHotEncoder`, which learned how many categories each
column has. Finally, the `LinearRegression` learned the coefficients and the intercept. The
`VectorAssembler` is a plain transformer because it only puts the columns together and doesn't
learn anything.

---

## Design: this pipeline in production

One paragraph. The platform runs this for real: thousands of rides a minute from Kafka, a
model retrained every night on the trips of the last month, anomalies sent to a fraud team.
Where do the training job, the scoring job, the checkpoint and the model live, how does a
new model reach the running scoring job without losing events, and what would you monitor
to know that the model has gone stale?

The training job would run every night as a batch job that reads the trips of the last month,
fits the pipeline and saves the model to storage like S3. The scoring job would be a Structured
Streaming job running all the time on the cluster, reading the rides from Kafka and sending the
anomalies to the fraud team. The checkpoint would also be on S3, so it survives a restart. To use
a new model, the scoring job is restarted with the new model and the same checkpoint, so it
continues from the last Kafka offset it processed and no ride is lost. To know if the model has
gone stale, I'd watch the RMSE against the real fares and check how many rides get flagged
per window.

---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

I had the same error as in L11, where the job failed with
`ModuleNotFoundError: No module named 'numpy'`, because the `apache/spark:4.2.0` image doesn't
come with numpy. So I just installed it again in the master and in both workers before running,
and all four parts ran correctly.
