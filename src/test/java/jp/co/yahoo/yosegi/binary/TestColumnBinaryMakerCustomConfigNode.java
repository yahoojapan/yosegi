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
package jp.co.yahoo.yosegi.binary;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import jp.co.yahoo.yosegi.compressor.DefaultCompressor;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.message.parser.IParser;

import jp.co.yahoo.yosegi.binary.maker.UnsupportedColumnBinaryMaker;

public class TestColumnBinaryMakerCustomConfigNode{

  @Test
  public void T_newInstance_1() throws IOException{
    InputStream in = this.getClass().getClassLoader().getResource( "binary/TestColumnBinaryMakerCustomConfigNode_1.json" ).openStream();
    BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
    JacksonMessageReader jacksonReader = new JacksonMessageReader();
    IParser jsonParser = jacksonReader.create( br.readLine() );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( defaultConfig , jsonParser );

    ColumnBinaryMakerConfig rootConfig = configNode.getCurrentConfig();
    assertEquals( defaultConfig.compressorClass.getClass().getName() , rootConfig.compressorClass.getClass().getName() );
    assertEquals( defaultConfig.unionMakerClass.getClass().getName() , rootConfig.unionMakerClass.getClass().getName() );
    assertEquals( defaultConfig.arrayMakerClass.getClass().getName() , rootConfig.arrayMakerClass.getClass().getName() );
    assertEquals( defaultConfig.spreadMakerClass.getClass().getName() , rootConfig.spreadMakerClass.getClass().getName() );
    assertEquals( defaultConfig.booleanMakerClass.getClass().getName() , rootConfig.booleanMakerClass.getClass().getName() );
    assertEquals( defaultConfig.byteMakerClass.getClass().getName() , rootConfig.byteMakerClass.getClass().getName() );
    assertEquals( defaultConfig.bytesMakerClass.getClass().getName() , rootConfig.bytesMakerClass.getClass().getName() );
    assertEquals( defaultConfig.doubleMakerClass.getClass().getName() , rootConfig.doubleMakerClass.getClass().getName() );
    assertEquals( defaultConfig.floatMakerClass.getClass().getName() , rootConfig.floatMakerClass.getClass().getName() );
    assertEquals( defaultConfig.integerMakerClass.getClass().getName() , rootConfig.integerMakerClass.getClass().getName() );
    assertEquals( defaultConfig.longMakerClass.getClass().getName() , rootConfig.longMakerClass.getClass().getName() );
    assertEquals( defaultConfig.shortMakerClass.getClass().getName() , rootConfig.shortMakerClass.getClass().getName() );

    assertEquals( configNode.getColumnName() , "root" );

    ColumnBinaryMakerCustomConfigNode pagedataConfigNode = configNode.getChildConfigNode( "pagedata" );
    ColumnBinaryMakerConfig pagedataConfig = pagedataConfigNode.getCurrentConfig();
    assertEquals( defaultConfig.compressorClass.getClass().getName() , pagedataConfig.compressorClass.getClass().getName() );
    assertEquals( defaultConfig.unionMakerClass.getClass().getName() , pagedataConfig.unionMakerClass.getClass().getName() );
    assertEquals( defaultConfig.arrayMakerClass.getClass().getName() , pagedataConfig.arrayMakerClass.getClass().getName() );
    assertEquals( defaultConfig.spreadMakerClass.getClass().getName() , pagedataConfig.spreadMakerClass.getClass().getName() );
    assertEquals( defaultConfig.booleanMakerClass.getClass().getName() , pagedataConfig.booleanMakerClass.getClass().getName() );
    assertEquals( defaultConfig.byteMakerClass.getClass().getName() , pagedataConfig.byteMakerClass.getClass().getName() );
    assertEquals( defaultConfig.bytesMakerClass.getClass().getName() , pagedataConfig.bytesMakerClass.getClass().getName() );
    assertEquals( defaultConfig.doubleMakerClass.getClass().getName() , pagedataConfig.doubleMakerClass.getClass().getName() );
    assertEquals( defaultConfig.floatMakerClass.getClass().getName() , pagedataConfig.floatMakerClass.getClass().getName() );
    assertEquals( defaultConfig.integerMakerClass.getClass().getName() , pagedataConfig.integerMakerClass.getClass().getName() );
    assertEquals( defaultConfig.longMakerClass.getClass().getName() , pagedataConfig.longMakerClass.getClass().getName() );
    assertEquals( defaultConfig.shortMakerClass.getClass().getName() , pagedataConfig.shortMakerClass.getClass().getName() );

    ColumnBinaryMakerCustomConfigNode queryConfigNode = pagedataConfigNode.getChildConfigNode( "query" );
    ColumnBinaryMakerConfig queryConfig = queryConfigNode.getCurrentConfig();
    assertEquals( DefaultCompressor.class.getName() , queryConfig.compressorClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.unionMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.arrayMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.spreadMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.booleanMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.byteMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.bytesMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.doubleMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.floatMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.integerMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.longMakerClass.getClass().getName() );
    assertEquals( UnsupportedColumnBinaryMaker.class.getName() , queryConfig.shortMakerClass.getClass().getName() );

  }

  @Test
  public void T_newInstance_2() throws IOException{
    InputStream in = this.getClass().getClassLoader().getResource( "binary/TestColumnBinaryMakerCustomConfigNode_2.json" ).openStream();
    BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
    JacksonMessageReader jacksonReader = new JacksonMessageReader();
    IParser jsonParser = jacksonReader.create( br.readLine() );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    assertThrows( IOException.class ,
      () -> {
        ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( defaultConfig , jsonParser );
      }
    );
  }

  @Test
  public void T_newInstance_3() throws IOException{
    InputStream in = this.getClass().getClassLoader().getResource( "binary/TestColumnBinaryMakerCustomConfigNode_3.json" ).openStream();
    BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
    JacksonMessageReader jacksonReader = new JacksonMessageReader();
    IParser jsonParser = jacksonReader.create( br.readLine() );

    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    assertThrows( IOException.class ,
      () -> {
        ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( defaultConfig , jsonParser );
      }
    );
  }

  @Test
  public void T_newInstance_4() throws IOException{
    ColumnBinaryMakerConfig defaultConfig = new ColumnBinaryMakerConfig();
    ColumnBinaryMakerCustomConfigNode configNode = new ColumnBinaryMakerCustomConfigNode( "root" , defaultConfig );
    assertNull( configNode.getChildConfigNode( "hoge" ) );
  }

}
