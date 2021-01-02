# Project Summary!
***
## Introduction
***

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

***
## Technology Stack
***
This Project uses following open source technologies to work properly:

* [Apache Spark](https://spark.apache.org/) - Apache Spark™ is a unified analytics engine for large-scale data processing.

* [Amazon S3](https://aws.amazon.com/s3/) - Amazon Simple Storage Service (Amazon S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance
* [Python](https://www.python.org/downloads/release/python-360/) - Programming language used in this project to build ETL pipelines and following are the libraries used in the project:
    -  [ConfigParser](https://pymotw.com/2/ConfigParser) - Use the ConfigParser module to manage user-editable configuration files for an application. The configuration files are organized into sections, and each section can contain name-value pairs for configuration data. Value interpolation using Python formatting strings is also supported, to build values that depend on one another (this is especially handy for URLs and message strings).
    -  [pyspark.sql](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html) - PySpark SQL is a module in Spark which integrates relational processing with Spark's functional programming API. We can extract the data by using an SQL query language. We can use the queries same as the SQL language.

***
### Code Deliverables:
***

1. **dl.cfg** Its a configuration file which can be used to configure AWS credentials and S3 file paths.
2. **etl.py** reads and processes files from song_data and log_data and loads them into various parquet format files. 
3. **Code-flow.ipynb** Notebook to test the code flow. 
4. **README.md** provides discussion on your project.


### License

None