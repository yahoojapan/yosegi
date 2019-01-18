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
package jp.co.yahoo.yosegi.message.parser.text;

import jp.co.yahoo.yosegi.message.design.ArrayContainerField;
import jp.co.yahoo.yosegi.message.design.LongField;
import jp.co.yahoo.yosegi.message.design.Properties;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.parser.IParser;

public class TestTextStreamReader {

  @Test
  public void T_reader() throws Exception {
    Properties properties = new Properties();
    properties.set( "delimiter" , "0x2c" );
    ArrayContainerField schema = new ArrayContainerField( "array" , new LongField( "array_value" ) , properties );

    byte[] dummyData = "100,200,300\n1,2,3\n10,20,30\n999,999,999\n".getBytes("UTF-8");
    InputStream in = new ByteArrayInputStream(dummyData);

    TextStreamReader reader = new TextStreamReader(in, schema);
    while (reader.hasNext()){
      IParser out = reader.next();
      System.out.println(out.get(2).getString());
    }

  }

}
