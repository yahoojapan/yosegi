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
package jp.co.yahoo.yosegi.compressor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import jp.co.yahoo.yosegi.spread.column.ColumnType;

public class TestCompressResult {

  @Test
  public void T_1() {
    CompressResult cr = new CompressResult( CompressionPolicy.DEFAULT , (double)1.2 );
    assertEquals( cr.getCurrentLevel() , 0 );
    cr.feedBack( 100 , 10 );
    assertEquals( cr.getCurrentLevel() , 1 );
    cr.feedBack( 100 , 11 );
    assertEquals( cr.getCurrentLevel() , 2 );
    cr.feedBack( 100 , 12 );
    assertEquals( cr.getCurrentLevel() , 1 );
    cr.feedBack( 100 , 20 );
    assertEquals( cr.getCurrentLevel() , 1 );
  }

  @Test
  public void T_2() {
    CompressResult cr = new CompressResult( CompressionPolicy.DEFAULT , (double)1.2 );
    assertEquals( cr.getCurrentLevel() , 0 );
    cr.feedBack( 10 ,  -100 );
    assertEquals( cr.getCurrentLevel() , 0 );
    cr.feedBack( 10 ,  100 );
    assertEquals( cr.getCurrentLevel() , 1 );
    cr.feedBack( 10 ,  -100 );
    assertEquals( cr.getCurrentLevel() , 1 );
    cr.feedBack( 10 ,  120 );
    assertEquals( cr.getCurrentLevel() , 0 );
  }

  @Test
  public void T_3() {
    CompressResult cr = new CompressResult( CompressionPolicy.DEFAULT , (double)1.2 );
    assertEquals( cr.getCurrentLevel() , 0 );
    cr.feedBack( 10 ,  100 );
    assertEquals( cr.getCurrentLevel() , 1 );
    cr.feedBack( 10 ,  110 );
    assertEquals( cr.getCurrentLevel() , 2 );
    cr.setEnd();
    assertEquals( cr.getCurrentLevel() , 1 );
  }

}
