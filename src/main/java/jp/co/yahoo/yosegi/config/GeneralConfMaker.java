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

package jp.co.yahoo.yosegi.config;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class GeneralConfMaker implements IConfigReader,IConfigWriter {

  private static final byte[] LINE_SEP = {'\n'};

  @Override
  public void read( final Map<String,String> settingContainer , final String confFilePath ) 
      throws IOException {
    InputStream in = new BufferedInputStream( new FileInputStream( confFilePath ) );
    read( settingContainer , in );
  }

  @Override
  public void read( final Map<String,String> settingContainer , final InputStream in ) 
      throws IOException {
    BufferedReader lineReader = new BufferedReader( new InputStreamReader( in , "UTF-8" ) );
    String line;
    while ( ( line = lineReader.readLine() ) != null ) {
      line = line.trim();
      if ( line.isEmpty() || line.indexOf("#") == 0 || line.indexOf("//") == 0 ) {
        continue;
      }
      String[] keyValue = line.split( "=" , 2 );
      if ( keyValue.length != 2 ) {
        throw new IOException( "Invalid str : " + line );
      }
      String key = keyValue[0].trim();
      String value = keyValue[1].trim();
      settingContainer.put( key , value );
    }
    lineReader.close();
  }

  @Override
  public void write( final Map<String,String> settingContainer ,
      final String outputPath ) throws IOException {
    write( settingContainer , outputPath , false );
  }

  @Override
  public void write( final Map<String,String> settingContainer ,
      final String outputPath , final boolean overwrite ) throws IOException {
    File targetFile = new File( outputPath );
    if ( targetFile.exists() ) {
      if ( overwrite ) {
        if ( ! targetFile.delete() ) {
          throw new IOException( "Could not remove file. Target : " + outputPath );
        }
      } else {
        throw new IOException( "Output file is already exists. Target : " + outputPath );
      }
    }

    OutputStream out = new BufferedOutputStream( new FileOutputStream( outputPath ) );
    write( settingContainer , out );
  }

  @Override
  public void write( final Map<String,String> settingContainer , final OutputStream out )
      throws IOException {
    Set<String> keySet = settingContainer.keySet();
    Iterator<String> keys = keySet.iterator();
    while ( keys.hasNext() ) {
      String key = keys.next();
      String value = settingContainer.get( key );
      byte[] outputBytes = String.format( "%s=%s" , key , value ).getBytes( "UTF-8" );
      out.write( outputBytes , 0 , outputBytes.length );
      out.write( LINE_SEP , 0 , LINE_SEP.length );
    }
    out.close();
  }

}
