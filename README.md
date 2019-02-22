<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
# Introduction to Yosegi
## What does this project do?
Yosegi is a Schema-less columnar storage format.
Provide flexible representation like JSON and efficient reading
similar to other columnar storage formats.


## Why is this project useful?
There was a problem that it is too large to compress
and save the data as it is in the Big Data era.
From the demand for improvement in compression ratio and read performance,
several columnar data formats (for example, Apache ORC and Apache Parquet)
were proposed.
They achieve the high compression ratio from similar data in column
and reading performance for grouping data by column when data is used.

However, these data formats are required
the data structure in a row (or a record) should be defined
before saving the data.
It was necessary to decide how to use it at the time of data storage,
and it was often a problem that it was difficult to decide
what kind of data to use.

In this project, we provide a new columnar format
which does not require the schema at the time of data storage
with compression and read performance equal to (or higher in case)
than other formats.


## Use cases
### Data Analysis
Analyzing big data requires store data compactly and get data smoothly.
Yosegi as a columnar format is useful for this needs.

### Data Lake
Data Lake is a data pool that is not required the data structure
(as a schema) in the row at the time of data storage.
And stored data can be used with defining its schema at the time of analyzing.
See [DataLake](https://en.wikipedia.org/wiki/Data_lake).

# License
This project is on the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).
Please treat this project under this license.

# How do I get started?

## Java
For easy usage please see the [quick start](docs/java/quickstart.md).

## CLI
Please see the repository of [yosegi-tools](https://github.com/yahoojapan/yosegi-tools) for details.

If you want to know what kind of function it has, look at the [command list](https://github.com/yahoojapan/yosegi-tools/blob/master/docs/command_list.md).

## Apache Hadoop

## Apache Hive
Yosegi supports Apache Hive.
Please see the repository of [yosegi-hive](https://github.com/yahoojapan/yosegi-hive) for details.

For easy usage please see [quick start](https://github.com/yahoojapan/yosegi-hive/blob/master/docs/quickstart.md).

## Apache Spark
Yosegi supports Apache Spark.
Please see the repository of [yosegi-spark](https://github.com/yahoojapan/yosegi-spark) for details.

For easy usage please see [quick start](https://github.com/yahoojapan/yosegi-spark/blob/master/docs/quickstart.md).

## Where can I get more help, if I need it?
Support and discussion of Yosegi are on the Mailing list.

* Mailing list: yosegi@googlegroups.com
* Bug trackter:[JIRA](https://yosegi.atlassian.net/projects/YOSEGI)

We plan to support and discussion of Yosegi on the Mailing list.
However, please contact us via GitHub until ML is opened.

# How to contribute
We welcome to join this project widely.

For information on how to start contributing to the project, please refer to the [Yosegi contribution guide](CONTRIBUTING.md).

# Building

## System requirement
Following environments are required.

* Mac OS X or Linux
* Java 8 Update 92 or higher (8u92+), 64-bit
* Maven 3.3.9 or later (for building)

## Maven
Yosegi sources can get from the Maven repository.

## Compile sources
Compile each source following instructions.

    $ mvn clean install
