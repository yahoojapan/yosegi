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
package jp.co.yahoo.yosegi.writer;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.config.Configuration;

public class TestYosegiSchemaStreamWriter{

  @Test
  public void T_new_Instance() throws IOException{
    YosegiSchemaStreamWriter writer;
    for( int i = 0 ; i < 100 ; i++ ){
      writer = new YosegiSchemaStreamWriter( new ByteArrayOutputStream() , new Configuration() );
      Map<Object,Object> data = new HashMap<Object,Object>();
      data.put( "hoge" , new IntegerObj(1) );
      writer.write( data );
      writer.close();
    }
  }

  @Test
  public void T_write_1() throws IOException{
    YosegiSchemaStreamWriter writer = new YosegiSchemaStreamWriter( new ByteArrayOutputStream() , new Configuration() );
    assertThrows( UnsupportedOperationException.class ,
      () -> {
        writer.write( new StringObj( "hoge" ) );
      }
    );
  }

  @Test
  public void T_write_2() throws IOException{
    YosegiSchemaStreamWriter writer = new YosegiSchemaStreamWriter( new ByteArrayOutputStream() , new Configuration() );
    assertThrows( UnsupportedOperationException.class ,
      () -> {
        writer.write( new ArrayList<Object>() );
      }
    );
  }

  @Test
  public void T_write_3() throws IOException{
    YosegiSchemaStreamWriter writer = new YosegiSchemaStreamWriter( new ByteArrayOutputStream() , new Configuration() );
    Map<Object,Object> data = new HashMap<Object,Object>();
    data.put( "hoge" , new IntegerObj(1) );
    writer.write( data );
    writer.close();
  }

  @Test
  public void T_write_4() throws IOException{
    YosegiSchemaStreamWriter writer = new YosegiSchemaStreamWriter( new ByteArrayOutputStream() , new Configuration() );

    JacksonMessageReader messageReader = new JacksonMessageReader();
    BufferedReader in = new BufferedReader( new InputStreamReader( this.getClass().getClassLoader().getResource( "formatter/TestYosegiSchemaStreamWriter.json" ).openStream() ) );
    String line = in.readLine();
    while( line != null ){
      IParser parser = messageReader.create( line );
      writer.write( parser );
      line = in.readLine();
    }
    writer.close();
  }
}
