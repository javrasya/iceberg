/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.source.split;

import java.io.IOException;
import java.io.Serializable;
import java.io.UTFDataFormatException;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/**
 * Helper class to serialize and deserialize strings longer than 65K. The inspiration is mostly
 * taken from the class org.apache.flink.core.memory.DataInputSerializer.readUTF and
 * org.apache.flink.core.memory.DataOutputSerializer.writeUTF.
 */
public class SerializerHelper implements Serializable {

  /** */
  private static final int BIGGEST_CHARACTER_SIZE_IN_BYTES = 3;

  /**
   * Maximum length of a serialized UTF string. This is the maximum length of a string that can be
   * encoded
   */
  private static final int MAX_SERIALIZED_UTF_LENGTH = 65535 / BIGGEST_CHARACTER_SIZE_IN_BYTES;

  /**
   * Writes a string to the given output view as chunks safely without hitting the maximum possible
   * length which can fit in an unsigned short which is 65kb. There are special characters which can
   * take up more space than 1 byte and the biggest one is 3 bytes (Those characters > 0x07FF). So
   * the provided string is turns into chunks size of 65kb/3 which covers the worst case scenario
   * where all the characters in the string are 3 bytes long.
   *
   * @param out The output view to write to.
   * @param str The string to be written as chunks.
   * @throws IOException Thrown, if the serialization encountered an I/O related error.
   */
  public static void writeUTFAsChunks(DataOutputSerializer out, String str) throws IOException {
    int numberOfChunks = (str.length() / MAX_SERIALIZED_UTF_LENGTH) + 1;
    out.writeShort(numberOfChunks);
    for (int i = 0; i < numberOfChunks; ++i) {
      int start = i * MAX_SERIALIZED_UTF_LENGTH;
      int end = Math.min((i + 1) * MAX_SERIALIZED_UTF_LENGTH, str.length());
      String chunk = str.substring(start, end);
      out.writeUTF(chunk);
    }
  }

  /**
   * Reads a string from the given input view chunk by chunk to build the whole text. The string was
   * previously written using the writeUTFAsChunks method.
   *
   * @param in The input view to read from.
   * @return The string that has been read.
   * @throws IOException Thrown, if the deserialization encountered an I/O related error.
   */
  public static String readUTFFromChunks(DataInputDeserializer in) throws IOException {
    int numberOfChunks = in.readShort();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < numberOfChunks; ++i) {
      builder.append(in.readUTF());
    }
    return builder.toString();
  }
}
