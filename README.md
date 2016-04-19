# ShpJob

Simple Hadoop map only job to test the [Shapefile](https://github.com/mraad/Shapefile) library.
This job reads a set of polygons in HDFS and emits the centroid locations and the names associated with each polygon.  

## Data Setup

Unzip `WorldAdmin.zip` in the `data` folder and place it in HDFS:

```
cd data
unzip WorldAdmin.zip && rm WorldAdmin.zip
hadoop fs -mkdir WorldAdmin
hadoop fs -put WorldAdmin.* WorldAdmin
```

## Building the Job

```
mvn package
```

## Running the Job

```
hadoop fs -rm -r -skipTrash /tmp/output
hadoop jar target/ShpJob-1.0-job.jar WorldAdmin /tmp/output
hadoop fs -cat /tmp/output/part*
```
