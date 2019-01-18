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

package jp.co.yahoo.yosegi.spread.expand;

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class ExpandFunctionFactory {

  private ExpandFunctionFactory() {}

  /**
   * Read Configuration and create processing related to expansion.
   */
  public static IExpandFunction get( final Configuration config ) throws IOException {
    String expandJson = config.get( "spread.reader.expand.column" , null );
    if ( expandJson == null || expandJson.isEmpty() ) {
      return new NotExpandFunction();
    }
    JacksonMessageReader jsonReader = new JacksonMessageReader();
    IParser jsonParser = jsonReader.create( expandJson );
    if ( ! jsonParser.containsKey( "base" ) ) {
      throw new IOException( "Invalid expand setting. \"base\" setting is not found." );
    }

    return parseBaseJson( jsonParser );
  }

  private static IExpandFunction parseBaseJson( final IParser jsonParser ) throws IOException {
    Set<String> baseLinkNameSet = new HashSet<String>();
    IParser currentBaseParser = jsonParser.getParser( "base" );
    List<String> columnNameList = new ArrayList<String>();
    List<String> linkColumnNameList = new ArrayList<String>();
    while ( true ) {
      columnNameList.add( currentBaseParser.get( "node" ).getString() );
      String linkName = currentBaseParser.get( "link_name" ).getString();
      if ( linkName != null ) {
        baseLinkNameSet.add( linkName );
      }
      linkColumnNameList.add( linkName );
      if ( ! currentBaseParser.containsKey( "child_node" ) ) {
        break;
      }
      currentBaseParser = currentBaseParser.getParser( "child_node" );
    }
    ExpandNode expandNode = new ExpandNode( columnNameList , linkColumnNameList );

    IParser parallelParser = jsonParser.getParser( "parallel" );
    ExpandColumnLink columnLink = new ExpandColumnLink();
    for ( int i = 0 ; i < parallelParser.size() ; i++ ) {
      IParser linkColumnParser = parallelParser.getParser( i );
      String baseLinkName = linkColumnParser.get( "base_link_name" ).getString();
      if ( ! baseLinkNameSet.contains( baseLinkName ) ) {
        throw new IOException( 
            String.format(
              "Invalid expand parallel setting. base link name \"%s\" is not defind." ,
              baseLinkName ) );
      }
      IParser nodesParser = linkColumnParser.getParser( "nodes" );
      String[] nodeNameArray = new String[nodesParser.size()];
      for ( int n = 0 ; n < nodesParser.size() ; n++ ) {
        nodeNameArray[n] = nodesParser.get(n).getString();
      }
      if ( nodeNameArray.length == 0 ) {
        throw new IOException( "Invalid expand parallel setting." );
      }
      String linkName = linkColumnParser.get( "link_name" ).getString();
      columnLink.addLinkColumn( new LinkColumn( baseLinkName , linkName , nodeNameArray ) );
    }

    return new ExpandFunction( expandNode , columnLink , columnNameList , linkColumnNameList );
  }

}
