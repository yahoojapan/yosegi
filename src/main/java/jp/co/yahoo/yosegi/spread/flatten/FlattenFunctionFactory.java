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
import jp.co.yahoo.yosegi.util.replacement.IReplacement;
import jp.co.yahoo.yosegi.util.replacement.PrefixAndSuffix;
import jp.co.yahoo.yosegi.util.replacement.ReplacementFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class FlattenFunctionFactory {

  private FlattenFunctionFactory() {}

  /**
   * Create flattening process from Configuration.
   */
  public static IFlattenFunction get( final Configuration config ) throws IOException {
    Iterator<String> iterator = config.getKey().iterator();
    List<String> targetKeyNameList = new ArrayList<String>();
    while ( iterator.hasNext() ) {
      String keyName = iterator.next();
      if ( ! keyName.startsWith( "spread.reader.flatten.column" ) ) {
        continue;
      }
      String flattenJson = config.get( keyName , null );
      if ( flattenJson == null ) {
        continue;
      }
      targetKeyNameList.add( keyName );
    }

    if ( targetKeyNameList.isEmpty() ) {
      return new NotFlattenFunction();
    }
    Collections.sort( targetKeyNameList );
    
    FlattenFunction flattenFunction = new FlattenFunction();
    for ( String keyName : targetKeyNameList ) {
      String flattenJson = config.get( keyName , null );
      JacksonMessageReader jsonReader = new JacksonMessageReader();
      IParser jsonParser = jsonReader.create( flattenJson );
      parseJson( jsonParser , flattenFunction );
    }

    return flattenFunction;
  }

  private static void parseJson(
      final IParser jsonParser ,
      final FlattenFunction flattenFunction ) throws IOException {
    if ( ! jsonParser.containsKey( "version" )
        || jsonParser.get( "version" ).getString().isEmpty() ) {
      parseJsonV1( jsonParser , flattenFunction );
      return;
    }
    String version = jsonParser.get( "version" ).getString();
    switch ( version ) {
      case "2":
        parseJsonV2( jsonParser , flattenFunction );
        return;
      default:
        throw new IOException(
            String.format( "The Flatten configuration version %s is incorrect." , version ) );
    }
  }

  private static void parseJsonV1(
      final IParser jsonParser ,
      final FlattenFunction flattenFunction ) throws IOException {
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
  }

  private static void parseJsonV2(
      final IParser jsonParser ,
      final FlattenFunction flattenFunction ) throws IOException {
    String version = jsonParser.get( "replacement" ).getString();
    IReplacement replacement = ReplacementFactory.get( version );
    IParser flattenSettingParser = jsonParser.getParser( "flatten" );
    for ( int i = 0 ; i < flattenSettingParser.size() ; i++ ) {
      IParser flattenColumnParser = flattenSettingParser.getParser(i);
      IParser nodeParser = flattenColumnParser.getParser( "column" );
      String[] nodeArray = new String[ nodeParser.size() ];
      for ( int n = 0 ; n < nodeParser.size() ; n++ ) {
        nodeArray[n] = nodeParser.get(n).getString();
      }
      String prefix = flattenColumnParser.get( "prefix" ).getString();
      String suffix = flattenColumnParser.get( "suffix" ).getString();
      String delimiter = flattenColumnParser.get( "delimiter" ).getString();
      if ( delimiter == null ) {
        delimiter = "_";
      }
      flattenFunction.setDelimiter( delimiter );
      PrefixAndSuffix prefixAndSuffixAppender = new PrefixAndSuffix( prefix , suffix , delimiter );
      IParser targetParser = flattenColumnParser.getParser( "target" );
      for ( int n = 0 ; n < targetParser.size() ; n++ ) {
        String[] childNodeArray = new String[ nodeArray.length + 1 ];
        childNodeArray[ childNodeArray.length - 1 ] = targetParser.get(n).getString();
        System.arraycopy( nodeArray , 0 , childNodeArray , 0 , nodeArray.length );
        String linkName = prefixAndSuffixAppender.append(
            replacement.replace( childNodeArray[ childNodeArray.length - 1 ] ) );
        flattenFunction.add( new FlattenColumn( linkName , childNodeArray ) );
      }
    }
  }

}
