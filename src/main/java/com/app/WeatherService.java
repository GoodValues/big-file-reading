package com.app;

import com.app.model.AvgTempPerYear;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;


@Service
public class WeatherService {

    // just to simplify we hardcode the file directory
    private final String FILE_DIR = "src/main/resources/example_file.csv";
    private final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private final String DATAFRAME_VIEW_NAME = "temperature_data";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SparkSession spark;

    @Autowired
    public WeatherService(final SparkSession sparkSession) {
        this.spark = sparkSession;
    }

    public String calculateAvgTemperaturesForCity(final String city) {
        try {
            createDataframeWithRawData();
            final List<AvgTempPerYear> yearlyTemperatures = createDataframeWithYearlyTemperatures(city);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(yearlyTemperatures);
        } catch (final Exception e) {
            return "An error occurred while calculating average temperatures. Error message: " + e.getMessage();
        }
    }

    private void createDataframeWithRawData() {
        Dataset<Row> df = spark.read()
                .option("header", false)
                .option("delimiter", ";")
                .csv(FILE_DIR)
                .toDF("city", "date", "temperature")
                .withColumn("date", to_date(col("date"), DATE_FORMAT))
                .repartition(col("date"));

        df.createOrReplaceTempView(DATAFRAME_VIEW_NAME);
    }

    private List<AvgTempPerYear> createDataframeWithYearlyTemperatures(final String city) {
        final Dataset<Row> averageTemperatures = spark.sql("SELECT year(date) as year, round(avg(temperature), 1) as averageTemperature " +
                "FROM " + DATAFRAME_VIEW_NAME + " " +
                "WHERE upper(city) = '" + city.toUpperCase() + "'" +
                "GROUP BY year(date) " +
                "ORDER BY year");

        return averageTemperatures.as(Encoders.bean(AvgTempPerYear.class)).collectAsList();
    }
}
