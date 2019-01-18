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
package jp.co.yahoo.yosegi.message.parser.json;

import java.io.IOException;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.formatter.json.JacksonMessageWriter;

public class TestJacksonMessageReader{

  @Test
  public void T_example_record() throws IOException{
    String data = "{\"a\":123,\"b\":true,\"c\":{ \"c-1\": \"HOGEHOGE\" } , \"d\":[ 10 , \"20\" , 0.1 ]}";
    byte[] dataBytes = data.getBytes();
    JacksonMessageReader messageReader = new JacksonMessageReader();
    IParser parser = messageReader.create( dataBytes );

    System.out.println( parser.get( "a" ).getInt() );
    System.out.println( parser.get( "b" ).getBoolean() );
    System.out.println( parser.get( "c" ).getString() );
    System.out.println( parser.get( "d" ).getString() );

    IParser cParser = parser.getParser( "c" );
    System.out.println( cParser.get( "c-1" ).getString() );

    IParser dParser = parser.getParser( "d" );
    for( int i = 0 ; i < dParser.size() ; i++ ){
      System.out.println( dParser.get( i ).getDouble() );
    }

    JacksonMessageWriter messageWriter = new JacksonMessageWriter();
    System.out.println( "Parser to JSON" );
    System.out.println( new String( messageWriter.create( parser ) ) );
  }

  @Test
  public void T_example_array() throws IOException{
    String data = "[ 100 , \"200\" , 3.0]";
    byte[] dataBytes = data.getBytes();
    JacksonMessageReader messageReader = new JacksonMessageReader();
    IParser parser = messageReader.create( dataBytes );

    for( int i = 0 ; i < parser.size() ; i++ ){
      System.out.println( parser.get( i ).getDouble() );
    }

    JacksonMessageWriter messageWriter = new JacksonMessageWriter();
    System.out.println( "Parser to JSON" );
    System.out.println( new String( messageWriter.create( parser ) ) );
  }

}
