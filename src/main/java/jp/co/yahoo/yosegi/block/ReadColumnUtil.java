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

package jp.co.yahoo.yosegi.block;

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class ReadColumnUtil {

  private ReadColumnUtil() {}

  /**
   * Convert JSON object of setting of read column to Java object.
   */
  public static List<String[]> readColumnSetting( final String settingJson ) throws IOException {
    JacksonMessageReader jsonReader = new JacksonMessageReader();
    List<String[]> result = new ArrayList<String[]>();
    if ( settingJson == null || settingJson.isEmpty() ) {
      return result;
    }
    IParser jsonParser = jsonReader.create( settingJson );
    for ( int i = 0 ; i < jsonParser.size() ; i++ ) {
      IParser nodeArrayParser = jsonParser.getParser(i);
      String[] nodeArray = new String[nodeArrayParser.size()];
      for ( int n = 0 ; n < nodeArray.length ; n++ ) {
        nodeArray[n] = nodeArrayParser.get(n).getString();
      }
      if ( 0 < nodeArray.length ) {
        result.add( nodeArray );
      }
    }

    return result;
  }

}
