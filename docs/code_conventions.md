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

# coding conventions
Caution:
This code convention is made under discussion among the contributor.
So, please submit a ticket to discuss if you have a comment or an idea.

## Tests
Test class and method coding rule as follow,

### 1. Class name
1-1. Add a prefix of Test.

1-2. Target test class name. Or feature name that is not link the target class.

Example:
```
TestColumnBinary
```

### 2. Test method name
2-1. Add a prefix of "T".

2-2. Test method name separates following three parts with "\_"

2-2-1. Test target method or the feature name that you want to test.

2-2-2. Expected behavior. If there is nothing, insert void, etc.

2-2-3. Prerequisites, If not, do not write this part.

Example:
```
T_get_intValue
T_get_intValue_withNull
T_filterAndGetValue
T_filterAndGetValue_withPerfectMatch
T_get_throwsException_withObjectIsString
```

### 3. discussion log
2019/06/18 ver.1 [Create the Primitive convention](https://yosegi.atlassian.net/browse/YOSEGI-50)



