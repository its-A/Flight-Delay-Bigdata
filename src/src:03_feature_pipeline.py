from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def create_spark():
    return (
        SparkSession.builder
        .appName("FlightDelayFeaturePipeline")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def run_pipeline():
    spark = create_spark()
    print("Loading curated flight data into PySpark...")

    # Read from your curated Parquet files
    df = spark.read.parquet("data/curated/flights")

    # 1. Target Definition: High Delay Month (Delay Rate >= 20%)
    base_df = (
        df.filter(F.col("TotalArrivals") >= 10)  # Filter out trivial volume
        .withColumn("DelayRate", F.col("DelayedArrivals15") / F.col("TotalArrivals"))
        .withColumn("CancellationRate", F.col("CancelledArrivals") / F.col("TotalArrivals"))
        .withColumn("IsHighDelayRisk", (F.col("DelayRate") >= 0.20).cast("integer"))
    )

    # 2. Window Function: Lagged Operational Performance (Historical baseline)
    # What was this carrier's delay rate at this airport in the previous recorded month?
    route_window = (
        Window.partitionBy("Carrier", "Airport")
        .orderBy("Year", "Month")
    )

    featured_df = (
        base_df
        .withColumn("PriorMonthDelayRate", F.lag("DelayRate", 1).over(route_window))
        .withColumn("PriorMonthDelayRate", F.coalesce(F.col("PriorMonthDelayRate"), F.lit(0.18))) # Default median fill
        .withColumn("Quarter", ((F.col("Month") - 1) / 3 + 1).cast("integer"))
        .withColumn("IsSummerOrHolidayPeak", F.when(F.col("Month").isin([6, 7, 8, 12]), 1).otherwise(0))
    )

    print(f"Total dataset records for feature pipeline: {featured_df.count()}")

    # 3. Categorical Encoders
    carrier_indexer = StringIndexer(inputCol="Carrier", outputCol="CarrierIdx", handleInvalid="keep")
    airport_indexer = StringIndexer(inputCol="Airport", outputCol="AirportIdx", handleInvalid="keep")

    encoder = OneHotEncoder(
        inputCols=["CarrierIdx", "AirportIdx"],
        outputCols=["CarrierVec", "AirportVec"]
    )

    # 4. Feature Vector Assembly
    numeric_cols = [
        "TotalArrivals",
        "CancellationRate",
        "PriorMonthDelayRate",
        "Month",
        "Quarter",
        "IsSummerOrHolidayPeak"
    ]

    assembler = VectorAssembler(
        inputCols=numeric_cols + ["CarrierVec", "AirportVec"],
        outputCol="raw_features"
    )

    scaler = StandardScaler(
        inputCol="raw_features", 
        outputCol="features", 
        withStd=True, 
        withMean=False
    )

    # 5. ML Classifier: Gradient-Boosted Trees
    gbt = GBTClassifier(
        labelCol="IsHighDelayRisk", 
        featuresCol="features", 
        maxIter=20, 
        seed=42
    )

    # Build Pipeline
    pipeline = Pipeline(stages=[
        carrier_indexer,
        airport_indexer,
        encoder,
        assembler,
        scaler,
        gbt
    ])

    # 6. Train/Test Split (Temporal split or random split)
    train_df, test_df = featured_df.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set: {train_df.count()} rows | Test set: {test_df.count()} rows")

    # Fit Model
    print("Fitting PySpark Pipeline and training GBT model...")
    model = pipeline.fit(train_df)

    # Evaluate
    predictions = model.transform(test_df)

    roc_evaluator = BinaryClassificationEvaluator(
        labelCol="IsHighDelayRisk", 
        rawPredictionCol="rawPrediction", 
        metricName="areaUnderROC"
    )
    pr_evaluator = BinaryClassificationEvaluator(
        labelCol="IsHighDelayRisk", 
        rawPredictionCol="rawPrediction", 
        metricName="areaUnderPR"
    )
    acc_evaluator = MulticlassClassificationEvaluator(
        labelCol="IsHighDelayRisk", 
        predictionCol="prediction", 
        metricName="accuracy"
    )

    auc_roc = roc_evaluator.evaluate(predictions)
    auc_pr = pr_evaluator.evaluate(predictions)
    accuracy = acc_evaluator.evaluate(predictions)

    print("\n" + "="*50)
    print("MODEL PERFORMANCE METRICS")
    print("="*50)
    print(f"Accuracy:        {accuracy * 100:.2f}%")
    print(f"ROC-AUC:         {auc_roc:.4f}")
    print(f"PR-AUC:          {auc_pr:.4f}")
    print("="*50 + "\n")

    # Save predictions sample to Mart for Tableau / reporting
    output_cols = [
        "Year", "Month", "Carrier", "Airport", 
        "TotalArrivals", "DelayRate", "IsHighDelayRisk", "prediction"
    ]
    predictions.select(output_cols).write.mode("overwrite").parquet("data/mart/delay_risk_predictions")
    print("Model predictions saved to data/mart/delay_risk_predictions")

    spark.stop()

if __name__ == "__main__":
    run_pipeline()