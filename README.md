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

## Apache Hadoop

## Apache Hive
Yosegi supports Apache Hive.
Please see the repository of [yosegi-hive](https://github.com/yahoojapan/yosegi-hive) for details.

For easy usage please see [quick start](https://github.com/yahoojapan/yosegi-hive/blob/master/docs/quickstart.md).

## Apache Spark

## Where can I get more help, if I need it?
Support and discussion of Yosegi are on the Mailing list.
Please refer the following subsection named "How to contribute".

We plan to support and discussion of Yosegi on the Mailing list.
However, please contact us via GitHub until ML is opened.

# How to contribute
We welcome to join this project widely.

## Mailing list
User support and discussion of Yosegi development are on the following Mailing list.
Please send a blank e-mail to the following address.

* address: yosegi@googlegroups.com
* subscribe: yosegi+subscribe@googlegroups.com
* unsubscribe: yosegi+unsubscribe@googlegroups.com

[Archive](https://groups.google.com/forum/#!forum/yosegi) is useful for what was communicated at this project.

## for Developer
Please accept [Contributer licence agreement](https://gist.github.com/ydnjp/3095832f100d5c3d2592)
when participating as a developer.

We invite you to [JIRA](https://yosegi.atlassian.net/projects/YOSEGI) as a bug tracking,
when you mentioned in the above Mailing list.

Please read the [developer document](docs/developing.md).

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
