# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf
import matplotlib.pyplot as plt
import logging
import warnings
import pandas as pd
from pyspark.ml.feature import MinMaxScaler, VectorAssembler

# Suppress warnings to ensure cleaner output logs
warnings.filterwarnings('ignore')

# Configure logging to suppress verbose output from Py4J
logging.getLogger('py4j').setLevel(logging.WARN)

# Configure Spark settings to optimize performance and suppress unnecessary UI progress bars
conf = SparkConf() \
    .set("spark.ui.showConsoleProgress", "false") \
    .set("spark.executor.heartbeatInterval", "200s") \
    .set("spark.network.timeout", "300s") \
    .set("spark.sql.shuffle.partitions", "8") \
    .set("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .set("spark.executor.memory", "2g") \
    .set("spark.driver.memory", "2g") \
    .set("spark.executor.cores", "2") \
    .set("spark.driver.cores", "2") \
    .set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/home/hduser/config/log4j.properties") \
    .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:/home/hduser/config/log4j.properties")

# Initialize Spark session with a specific application name and configuration
spark = SparkSession.builder.appName("Crime and Arrest Data Analysis").config(conf=conf).getOrCreate()

# Set Spark log level to WARN to minimize log output for cleaner execution visibility
spark.sparkContext.setLogLevel("WARN")

# Load datasets from HDFS and infer schema automatically
arrest_data = spark.read.csv("hdfs://localhost:54310/project/hduser/Arrest_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
crime_data = spark.read.csv("hdfs://localhost:54310/project/hduser/Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)

# Display the schemas of the datasets for verification
crime_data.printSchema()
arrest_data.printSchema()

# Data cleaning: Drop unnecessary columns to streamline the datasets for analysis
columns_to_drop_from_arrest = ["Report ID", "Arrestee Address", "Arrestee City", "Arrestee State", "Arrestee Zip Code", 
                               "Charge Group Code", "Charge Group Description", "Charge Description", 
                               "Cross Street", "LOCATION", "Area Name"]
columns_to_drop_from_crime = ["DR Number", "Date Reported", "Time Occurred", "MO Codes", "Premise Code", "Premise Description", 
                              "Weapon Used Code", "Weapon Description", "Status Code", "Status Description", "LAT", "LON"]

arrest_data = arrest_data.drop(*columns_to_drop_from_arrest)
crime_data = crime_data.drop(*columns_to_drop_from_crime)

# Join the datasets based on common columns to correlate data between arrests and crimes
merged_data = arrest_data.join(crime_data,
                           (arrest_data["Area ID"] == crime_data["AREA"]) &
                           (arrest_data["Reporting District"] == crime_data["Rpt Dist No"]) &
                           (arrest_data["Arrest Date"] == crime_data["DATE OCC"]),
                           "inner")

# Select only relevant columns for further analysis
merged_data = merged_data.select(arrest_data["*"], crime_data["*"])

# Convert to Pandas DataFrame for easier manipulation and visualization in Python
columns = ['Area Name','Area ID', 'Crm Cd Desc', 'Age', 'Vict Age', 'LAT', 'LON', 'DATE OCC']
merged_data = merged_data.select(columns)
crime_and_arrest_df = merged_data.toPandas()

# Plot and save the Number of Crimes by Area chart
crime_by_area = crime_and_arrest_df['Area Name'].value_counts().sort_index()
plt.figure(figsize=(10, 6))
crime_by_area.plot(kind='bar')
plt.title('Number of Crimes by Area')
plt.xlabel('Area Name')
plt.ylabel('Number of Crimes')
plt.savefig('Number_of_Crimes_by_Area.png')  # Save to your desired path
plt.close()

# Plot and save the Number of Arrests by Crime Type chart
arrest_by_crime_type = crime_and_arrest_df['Crm Cd Desc'].value_counts().sort_values()
plt.figure(figsize=(25, 12))
arrest_by_crime_type.plot(kind='bar')
plt.title('Number of Arrests by Crime Type')
plt.xlabel('Crime Type')
plt.ylabel('Number of Arrests')
plt.xticks(rotation=90)
plt.savefig('Number_of_Arrests_by_Crime_Type.png')  # Save to your desired path
plt.close()



# Additional exploratory data analysis (EDA) with summary statistics
crime_and_arrest_df.describe().to_csv('crime_and_arrest_summary_stats.csv')

# Calculate the number of arrests by crime type filtered by a threshold and save the plot
arrest_by_crime_type_filtered = arrest_by_crime_type[arrest_by_crime_type > 1000]  # filtering by more than 1000 arrests
plt.figure(figsize=(25, 12))
arrest_by_crime_type_filtered.plot(kind='bar')
plt.title('Number of Arrests by Crime Type (More than 1000 arrests)')
plt.xlabel('Crime Type')
plt.ylabel('Number of Arrests')
plt.xticks(rotation=90)
plt.savefig('Number_of_Arrests_by_Crime_Type_filtered.png')
plt.close()

# Feature assembly and normalization for further machine learning applications
numeric_columns = ['Age', 'Vict Age', 'LAT', 'LON']
assembler = VectorAssembler(inputCols=numeric_columns, outputCol='features')
data_assembled = assembler.transform(merged_data)

# Apply MinMaxScaler to scale the numeric features
scaler = MinMaxScaler(inputCol='features', outputCol='scaled_features')
scaler_model = scaler.fit(data_assembled)
data_scaled = scaler_model.transform(data_assembled)

# Save scaled data to CSV for further analysis
data_scaled.select('scaled_features').toPandas().to_csv('scaled_data.csv')

# Example of parallel processing using Spark to count number of crimes by area
crime_count_by_area = merged_data.groupBy('Area ID').count()
crime_count_by_area.show()

# Save crime count by area to CSV
crime_count_by_area.toPandas().to_csv('crime_count_by_area.csv')

# Using MapReduce for distributed computing analysis on crime data
def map_function(row):
    return (row['Area Name'], 1)

def reduce_function(a, b):
    return a + b

# Apply MapReduce to compute counts of crimes by area
crime_count_rdd = merged_data.rdd.map(map_function).reduceByKey(reduce_function)
crime_count_results = crime_count_rdd.collect()

# Display the results of the MapReduce computation
for result in crime_count_results:
    print(result)

# Convert results to a DataFrame for easier visualization and plotting as a pie chart
crime_count_df = pd.DataFrame(crime_count_results, columns=['Area Name', 'Count'])
plt.figure(figsize=(10, 10))
plt.pie(crime_count_df['Count'], labels=crime_count_df['Area Name'], autopct='%1.1f%%', startangle=140)
plt.title('Number of Crimes by Area (MapReduce)')
plt.axis('equal')
plt.savefig('Number_of_Crimes_by_Area_MapReduce.png')
plt.close()

# Summary insights: Display top crime areas, most common crime types, and trends over time
print("Top crime areas:")
top_crime_areas = crime_count_by_area.orderBy('count', ascending=False).limit(5)
top_crime_areas.show()

print("Most common crime types leading to arrests:")
common_crime_types = merged_data.groupBy('Crm Cd Desc').count().orderBy('count', ascending=False).limit(5)
common_crime_types.show()

print("Crime trends over time:")
crime_trends = merged_data.groupBy('DATE OCC').count().orderBy('DATE OCC')
crime_trends.show()

# Save these insights to a text file for documentation
with open('crime_insights.txt', 'w') as file:
    file.write("Top crime areas:\n")
    file.write(str(top_crime_areas.collect()) + "\n")
    file.write("Most common crime types leading to arrests:\n")
    file.write(str(common_crime_types.collect()) + "\n")
    file.write("Crime trends over time:\n")
    file.write(str(crime_trends.collect()) + "\n")

