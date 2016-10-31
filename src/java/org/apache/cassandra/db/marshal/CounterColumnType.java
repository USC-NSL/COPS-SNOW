/*
 *
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
 *
 */
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.CounterUpdateColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CounterColumnType extends AbstractCommutativeType
{
    public static final CounterColumnType instance = new CounterColumnType();

    CounterColumnType() {} // singleton

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (o1 == null)
            return null == o2 ?  0 : -1;

        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    @Override
    public String getString(ByteBuffer bytes)
    {
        return ByteBufferUtil.bytesToHex(bytes);
    }

    /**
     * create commutative column
     */
    @Override
    public Column createColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        return new CounterUpdateColumn(name, value, timestamp, null);
    }

    @Override
    public ByteBuffer fromString(String source)
    {
        return ByteBufferUtil.hexToBytes(source);
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 8 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 8 or 0 byte long (%d)", bytes.remaining()));
    }
}
