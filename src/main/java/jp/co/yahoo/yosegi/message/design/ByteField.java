/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.message.design;

public class ByteField implements IField {

  private final String name;
  private final Properties properties;

  public ByteField( final String name ) {
    this.name = name;
    properties = new Properties();
  }

  public ByteField( final String name ,  final Properties properties ) {
    this.name = name;
    this.properties = properties;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public FieldType getFieldType() {
    return FieldType.BYTE;
  }

}
