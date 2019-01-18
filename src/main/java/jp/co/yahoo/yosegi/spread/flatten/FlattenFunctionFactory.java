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

package jp.co.yahoo.yosegi.spread.flatten;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

import java.io.IOException;

public final class FlattenFunctionFactory {

  private FlattenFunctionFactory() {}

  /**
   * Create flattening process from Configuration.
   */
  public static IFlattenFunction get( final Configuration config ) throws IOException {
    String flattenJson = config.get( "spread.reader.flatten.column" , null );
    if ( flattenJson == null || flattenJson.isEmpty() ) {
      return new NotFlattenFunction();
    }
    JacksonMessageReader jsonReader = new JacksonMessageReader();
    IParser jsonParser = jsonReader.create( flattenJson );

    FlattenFunction flattenFunction = parseJson( jsonParser );
    if ( flattenFunction.isEmpty() ) {
      return new NotFlattenFunction();
    }

    return flattenFunction;
  }

  private static FlattenFunction parseJson( final IParser jsonParser ) throws IOException {
    FlattenFunction flattenFunction = new FlattenFunction();
    for ( int i = 0 ; i < jsonParser.size() ; i++ ) {
      IParser flattenColumnParser = jsonParser.getParser(i);
      String linkName = flattenColumnParser.get( "link_name" ).getString();
      if ( linkName == null || linkName.isEmpty() ) {
        throw new IOException( "Invalid flatten setting. link_name is null or empty." );
      }
      IParser nodeParser = flattenColumnParser.getParser( "nodes" );
      String[] nodeArray = new String[ nodeParser.size() ];
      for ( int n = 0 ; n < nodeParser.size() ; n++ ) {
        nodeArray[n] = nodeParser.get(n).getString();
      }
      flattenFunction.add( new FlattenColumn( linkName , nodeArray ) );
    }

    return flattenFunction;
  }

}
