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

# What is logical data size?

In Yosegi, data size is counted as statistics.
Yosegi counts three data sizes.
One is the data size when binary, the second is the data size when binary is compressed, and the third is the logical data size.
The data size defined here is the logical data size.

# Why define the data size?

Yosegi balances processing performance and compression efficiency.
Therefore, it is impossible to accurately measure the performance with the binary data size which varies depending on the implementation.
Define the logical data size to make it possible to measure the data amount of file, block, spread irrespective of binary state.
By using this statistic, we measure processing performance and compression efficiency to improve Yosegi performance.

Other than that, we are assuming use at data rake.
In Data Lake, it is often via a mechanism to gather event data occurring in real time.
If you know the logical data size when saving the data in Yosegi, you can also measure how much data was processed for the resource to be saved in the data rake.

# Definition of data size

| Data type | byte size |
|:-----------|:------------|
| NULL  | 0byte |
| Boolean | 1byte |
| Byte  | 1byte |
| Short | 2byte |
| Int   | 4byte |
| Long  | 8byte |
| Float | 4byte |
| Double| 8byte |
| String| 4byte + Byte * length |
| Binary| 4byte + Byte * length |
| Array | 8byte |
| Map,Struct| 0 byte |
