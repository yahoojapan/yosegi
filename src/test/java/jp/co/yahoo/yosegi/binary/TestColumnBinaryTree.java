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

import java.io.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.objects.*;
import jp.co.yahoo.yosegi.writer.*;
import jp.co.yahoo.yosegi.reader.*;
import jp.co.yahoo.yosegi.spread.*;

public class TestColumnBinaryTree{

  @Test
  public void T_emptyColumn_1() throws IOException{
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Configuration config = new Configuration();
    YosegiRecordWriter writer = new YosegiRecordWriter( out , config );

    Map<String,Object> m = new HashMap<String,Object>();
    m.put( "c1" , new StringObj( "a" ) );
    writer.addRow( m );
    writer.close();

    YosegiReader reader = new YosegiReader();
    Configuration readerConfig = new Configuration();
    byte[] data = out.toByteArray();
    InputStream fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    int line = 0;
    while( reader.hasNext() ){
      Spread spread = reader.next();
      line += spread.size();
    }
    assertEquals( 1 , line );
    readerConfig.set( "spread.reader.read.column.names" , "[ [ \"b\" ] ]" );
    fileIn = new ByteArrayInputStream( data );
    reader.setNewStream( fileIn , data.length , readerConfig );
    line = 0;
    while( reader.hasNext() ){
      Spread spread = reader.next();
      line += spread.size();
    }
    assertEquals( 1 , line );
  }


}
