package com.app;

import com.app.model.AvgTempPerYear;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.year;


@Service
public class WeatherService {

    // just to simplify we hardcode the file directory
    private final String FILE_DIR = "C:\\Users\\Adam\\Desktop\\big-file-reading\\src\\main\\resources";
    private final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private final String DATAFRAME_VIEW_NAME = "temperature_data";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SparkSession spark;
    private final StructType sparkSchema = new StructType()
            .add("city", DataTypes.StringType, false)
            .add("date", DataTypes.StringType, false)
            .add("temperature", DataTypes.DoubleType, false);

    @Autowired
    public WeatherService(final SparkSession sparkSession) throws TimeoutException, StreamingQueryException {
        this.spark = sparkSession;
        startProcessingTemperatureData();
    }

    public String calculateAvgTemperaturesForCity(final String city) {
        try {
            final List<AvgTempPerYear> yearlyTemperatures = getAverageTemperature(city);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(yearlyTemperatures);
        } catch (final Exception e) {
            return "An error occurred while calculating average temperatures. Error message: " + e.getMessage();
        }
    }

    public void startProcessingTemperatureData() throws TimeoutException, StreamingQueryException {
        final Dataset<Row> dataStream = readDataStream();
        final Dataset<Row> averageTemperatures = executeDataQuery(dataStream);
        averageTemperatures.createOrReplaceTempView(DATAFRAME_VIEW_NAME);
        writeDataStream(averageTemperatures);
    }

    private List<AvgTempPerYear> getAverageTemperature(final String city) {
        final List<AvgTempPerYear> result = new ArrayList<>();
        final Dataset<Row> averageTemperature = spark.sql("SELECT year, averageTemperature " +
                "FROM " + DATAFRAME_VIEW_NAME + " " +
                "WHERE upper(city) = '" + city.toUpperCase() + "' " +
                "ORDER BY year");

        for (final Row row : averageTemperature.collectAsList()) {
            result.add(new AvgTempPerYear(row.getString(0), row.getDouble(1)));
        }

        return result;
    }

    private Dataset<Row> readDataStream() {
        return spark.readStream()
                .option("header", false)
                .option("delimiter", ";")
                .format("csv")
                .schema(sparkSchema)
                .csv(FILE_DIR)
                .toDF("city", "date", "temperature")
                .withColumn("date", to_date(col("date"), DATE_FORMAT))
                .withColumn("year", year(col("date")))
                .drop(col("date"))
                .repartition(col("year"));
    }

    private Dataset<Row> executeDataQuery(final Dataset<Row> dataStream) {
        return dataStream.groupBy("year", "city")
                .agg(functions.avg("temperature").alias("averageTemperature"));
    }

    private void writeDataStream(final Dataset<Row> averageTemperatures) throws TimeoutException, StreamingQueryException {
        averageTemperatures.writeStream()
                .format("memory")
                .outputMode(OutputMode.Update())
                .queryName(DATAFRAME_VIEW_NAME)
                .start()
                .awaitTermination();
    }
}
