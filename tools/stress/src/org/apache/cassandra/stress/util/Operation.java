/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stress.util;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.Stress;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.stress.operations.ZipfianGenerator;

public abstract class Operation
{
    public final int index;

    protected final Session session;
    protected static volatile Double nextGaussian = null;

    public Operation(int idx)
    {
        index = idx;
        session = Stress.session;
    }

    public Operation(Session client, int idx)
    {
        index = idx;
        session = client;
    }

    /**
     * Run operation
     * @param client Cassandra Thrift client connection
     * @throws IOException on any I/O error.
     */
    public abstract void run(Cassandra.Client client) throws IOException;

    /**
     * Run operation
     * @param clientLibrary a COPS clientLibrary
     * @throws IOException on any I/O error.
     */
    public void run(ClientLibrary clientLibrary) throws IOException
    {
        throw new RuntimeException("This type of operation doesn't support being called via the ClientLibrary");
    }


    // Utility methods

    protected List<ByteBuffer> generateValues()
    {
        if (session.averageSizeValues)
        {
            return generateRandomizedValues();
        }

        List<ByteBuffer> values = new ArrayList<ByteBuffer>();

        for (int i = 0; i < session.getCardinality(); i++) {
            String hash = getMD5(Integer.toString(i));
            int times = session.getColumnSize() / hash.length();
            int sumReminder = session.getColumnSize() % hash.length();

            String value = new StringBuilder(multiplyString(hash, times)).append(hash.substring(0, sumReminder)).toString();
            values.add(ByteBuffer.wrap(value.getBytes()));
        }

        return values;
    }

    /**
     * Generate values of average size specified by -S, up to cardinality specified by -C
     * @return Collection of the values
     */
    protected List<ByteBuffer> generateRandomizedValues()
    {
        List<ByteBuffer> values = new ArrayList<ByteBuffer>();

        int limit = 2 * session.getColumnSize();

        for (int i = 0; i < session.getCardinality(); i++)
        {
            byte[] value = new byte[Stress.randomizer.nextInt(limit)];
            Stress.randomizer.nextBytes(value);
            values.add(ByteBuffer.wrap(value));
        }

        return values;
    }

    //Functions for generating stats that match FB
    protected ArrayList<ByteBuffer> generateFBValues()
    {
        ArrayList<ByteBuffer> values = new ArrayList<ByteBuffer>();

        int uniqueValues = Math.min(session.getCardinality(), 10000);
        for (int i = 0; i < uniqueValues; i++)
	{
	    String hash = getMD5(Integer.toString(i));

	    int columnSize = getFBColumnSize();

	    int times = columnSize / hash.length();
	    int sumReminder = columnSize % hash.length();

	    String value = new StringBuilder(multiplyString(hash, times)).append(hash.substring(0, sumReminder)).toString();
	    values.add(ByteBuffer.wrap(value.getBytes()));
	}
        return values;
    }

    //returns a random column size according to the FB distribution
    Random fbColumnSizeRandomizer;
    private int getFBColumnSize()
    {
	if (fbColumnSizeRandomizer == null)
	    fbColumnSizeRandomizer = new Random(0);
        double toss = fbColumnSizeRandomizer.nextDouble();

        //9.5% of reads are on objects
        if (toss < .095) {
            return getFBObjColumnSize();
        } else {
            return getAssocColumnSize();
        }
    }

    Random fbObjColumnSizeRandomizer;
    private int getFBObjColumnSize()
    {
	if (fbObjColumnSizeRandomizer == null)
	    fbObjColumnSizeRandomizer = new Random(1);
        //fbobj_data_size.hist
        double toss = fbObjColumnSizeRandomizer.nextDouble();

        //sizes are increasing powers of 2

        //empty fbobject we'll call 1 byte
        if (toss < 0.00704422) { return 1; }
        toss -= 0.00704422;

        //0 0 0
        //1 0 0

        if (toss < 0.000306422) { return 4; }
        toss -= 0.000306422;

        //3 0.00847108 104554
        if (toss < 0.00847108) { return 8; }
        toss -= 0.00847108;

        //4 0.0389854 481176
        if (toss < 0.0389854) { return 16; }
        toss -= 0.0389854;

        //5 0.239204 2952363
        if (toss < 0.239204) { return 32; }
        toss -= 0.239204;

        //6 0.195086 2407841
        if (toss < 0.195086) { return 64; }
        toss -= 0.195086;

        //7 0.209904 2590727
        if (toss < 0.209904) { return 128; }
        toss -= 0.209904;

        //8 0.177451 2190177
        if (toss < 0.177451) { return 256; }
        toss -= 0.177451;

        //9 0.0412025 508540
        if (toss < 0.0412025) { return 512; }
        toss -= 0.0412025;

        //10 0.0176862 218291
        if (toss < 0.0176862) { return 1024; }
        toss -= 0.0176862;

        //11 0.0201117 248228
        if (toss < 0.0201117) { return 2048; }
        toss -= 0.0201117;

        //12 0.0294145 363047
        if (toss < 0.0294145) { return 4096; }
        toss -= 0.0294145;

        //13 0.00809425 99903
        if (toss < 0.00809425) { return 8192; }
        toss -= 0.00809425;

        //14 0.00378539 46721
        if (toss < 0.00378539) { return 16384; }
        toss -= 0.00378539;

        //15 0.0022045 27209
        //16 0.000855583 10560
        //17 0.000162933 2011
        //18 3.12742e-05 386
        //call them all 2^15
        return 32768;
    }

    Random assocColumnSizeRandomizer;
    private int getAssocColumnSize()
    {
	if (assocColumnSizeRandomizer == null)
	    assocColumnSizeRandomizer = new Random(2);
        //assoc_data_size.hist
        double toss = assocColumnSizeRandomizer.nextDouble();

        //-1 0.384472 99465600
        if (toss < 0.384472) { return 1; }
        toss -= 0.384472;

        //0 0 0
        //1 0.100752 26065213
        if (toss < 0.100752) { return 2; }
        toss -= 0.100752;

        //2 0.0017077 441795
        if (toss < 0.0017077) { return 4; }
        toss -= 0.0017077;

        //3 0.00769552 1990885
        if (toss < 0.00769552) { return 8; }
        toss -= 0.00769552;

        //4 0.325119 84110720
        if (toss < 0.325119) { return 16; }
        toss -= 0.325119;

        //5 0.115914 29987896
        if (toss < 0.115914) { return 32; }
        toss -= 0.115914;

        //6 0.0365789 9463233
        if (toss < 0.0365789) { return 64; }
        toss -= 0.0365789;

        //7 0.0139127 3599313
        if (toss < 0.0139127) { return 128; }
        toss -= 0.0139127;

        //8 0.00197022 509710
        if (toss < 0.00197022) { return 256; }
        toss -= 0.00197022;

        //9 0.00137401 355467
        if (toss < 0.00137401) { return 512; }
        toss -= 0.00137401;

        //10 0.000139049 35973
        if (toss < 0.000139049) { return 1024; }
        toss -= 0.000139049;

        //11 0.000291774 75484
        if (toss < 0.000291774) { return 2048; }
        toss -= 0.000291774;

        //12 0.00515581 1333846
        if (toss < 0.00515581) { return 4096; }
        toss -= 0.00515581;

        //13 0.00491133 1270597
        if (toss < 0.00491133) { return 8192; }
        toss -= 0.00491133;

        //14 5.53908e-06 1433
        if (toss < 0.00000553908) { return 16384; }
        toss -= 0.00000553908;

        //15 7.73075e-09 2
        return 32768;
    }


    Random fbColumnCountRandomizer;
    protected int getFBColumnCount()
    {
	if (fbColumnCountRandomizer == null)
	    fbColumnCountRandomizer = new Random(3);

	return getFBColumnCount(fbColumnCountRandomizer);
    }

    protected int getFBColumnCount(Random randomizer)
    {
        //range_res_count.hist
        double toss = randomizer.nextDouble();

        //each category is for a power of 2 starting at 0, 1, 2, 4 ...

        //no columns were returned, but we'll say 1 for now
        //TODO: get an empty column here
        if (toss < 0.709204) { return 1; }
        toss -= 0.709204;

        if (toss < 0.185909) { return 1; }
        toss -= 0.185909;

        if (toss < 0.0222263) { return 2; }
        toss -= 0.0222263;

        if (toss < 0.0202094) { return 4; }
        toss -= 0.0202094;

        if (toss < 0.017849) { return 8; }
        toss -= 0.017849;

        if (toss < 0.0150605) { return 16; }
        toss -= 0.0150605;

        if (toss < 0.0110481) { return 32; }
        toss -= 0.0110481;

        if (toss < 0.00653806) { return 64; }
        toss -= 0.00653806;

        if (toss < 0.0080639) { return 128; }
        toss -= 0.0080639;

        if (toss < 0.00175157) { return 256; }
        toss -= 0.00175157;

        if (toss < 0.00104441) { return 512; }
        toss -= 0.00104441;

        //0.000681581 10 1546
        //0.00024909 11 565
        //8.90552e-05 12 202
        //7.3184e-05 13 166
        //2.20434e-06 14 5
        //we just lump these really unlikely sizes together
        return 1024;
    }

    Random fbReadBatchSizeRandomizer;
    protected int getFBReadBatchSize()
    {
	if (fbReadBatchSizeRandomizer == null)
	    fbReadBatchSizeRandomizer = new Random(4);
        //having trouble extracting the data, i'll eyeball it off the graph
        double toss = fbReadBatchSizeRandomizer.nextDouble();

        //0 = .515;
        if (toss < .515) { return 1; }
        toss -= .515;

        //1 = .1;
        if (toss < .1) { return 2; }
        toss -= .1;

        //2 = .12;
        if (toss < .12) { return 4; }
        toss -= .12;

        //3 = .1;
        if (toss < .1) { return 8; }
        toss -= .1;

        //4 = .08;
        if (toss < .08) { return 16; }
        toss -= .08;

        //5 = .05;
        if (toss < .05) { return 32; }
        toss -= .05;

        //6 = .02;
        if (toss < .02) { return 64; }
        toss -= .02;

        //7 = .008;
        if (toss < .008) { return 128; }
        toss -= .008;

        //8 = .004;
        if (toss < .004) { return 256; }
        toss -= .004;

        //9 = .002;
        if (toss < .002) { return 512; }
        toss -= .002;

        //+ = .001;
        return 1024;
    }


    /**
     * key generator using Gauss or Random algorithm
     * @return byte[] representation of the key string
     */
    protected byte[] generateKey()
    {
        return (session.useRandomGenerator()) ? generateRandomKey() : generateGaussKey();
    }

    /*
     * RO6: generate keys based on zipfian workload
     * called by DynamicWorkload.java
     */
    protected byte[] generateZipfianKey(int min, int max)
    {
        ZipfianGenerator zipfian = new ZipfianGenerator(min, max);
        String format = "%0" + session.getTotalKeysLength() + "d";
        return String.format(format, zipfian.nextInt(max-min+1)).getBytes(UTF_8);
    }

    /*
    * Khiem
    * RO6: generate keys based on zipfian workload
    * given the zipfian constant as parameter
    */
    protected byte[] generateZipfianKey(int min, int max, double zipfianconstant)
    {
	ZipfianGenerator zipfian = new ZipfianGenerator(min, max, zipfianconstant);
	String format = "%0" + session.getTotalKeysLength() + "d";
	return String.format(format, zipfian.nextInt(max-min+1)).getBytes(UTF_8);
    }


    /**
     * Random key generator
     * @return byte[] representation of the key string
     */
    private byte[] generateRandomKey()
    {
        String format = "%0" + session.getTotalKeysLength() + "d";
        return String.format(format, Stress.randomizer.nextInt(Stress.session.getNumDifferentKeys() - 1)).getBytes(UTF_8);
    }

    /**
     * Gauss key generator
     * @return byte[] representation of the key string
     */
    private byte[] generateGaussKey()
    {
        String format = "%0" + session.getTotalKeysLength() + "d";

        for (;;)
        {
            double token = nextGaussian(session.getMean(), session.getSigma());

            if (0 <= token && token < session.getNumDifferentKeys())
            {
                return String.format(format, (int) token).getBytes(UTF_8);
            }
        }
    }

    /**
     * Gaussian distribution.
     * @param mu is the mean
     * @param sigma is the standard deviation
     *
     * @return next Gaussian distribution number
     */
    private static double nextGaussian(int mu, float sigma)
    {
        Random random = Stress.randomizer;

        Double currentState = nextGaussian;
        nextGaussian = null;

        if (currentState == null)
        {
            double x2pi  = random.nextDouble() * 2 * Math.PI;
            double g2rad = Math.sqrt(-2.0 * Math.log(1.0 - random.nextDouble()));

            currentState = Math.cos(x2pi) * g2rad;
            nextGaussian = Math.sin(x2pi) * g2rad;
        }

        return mu + currentState * sigma;
    }

    /**
     * MD5 string generation
     * @param input String
     * @return md5 representation of the string
     */
    private String getMD5(String input)
    {
        MessageDigest md = FBUtilities.threadLocalMD5Digest();
        byte[] messageDigest = md.digest(input.getBytes(UTF_8));
        StringBuilder hash = new StringBuilder(new BigInteger(1, messageDigest).toString(16));

        while (hash.length() < 32)
            hash.append("0").append(hash);

        return hash.toString();
    }

    /**
     * Equal to python/ruby - 's' * times
     * @param str String to multiple
     * @param times multiplication times
     * @return multiplied string
     */
    private String multiplyString(String str, int times)
    {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < times; i++)
            result.append(str);

        return result.toString();
    }

    protected ByteBuffer columnName(int index, boolean timeUUIDComparator)
    {
        return timeUUIDComparator
                ? TimeUUIDType.instance.decompose(UUIDGen.makeType1UUIDFromHost(Session.getLocalAddress()))
                : ByteBufferUtil.bytes(String.format("C%d", index));
    }

    protected String getExceptionMessage(Exception e)
    {
        String className = e.getClass().getSimpleName();
        String message = (e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage();
        return (message == null) ? "(" + className + ")" : String.format("(%s): %s", className, message);
    }

    protected void error(String message) throws IOException
    {
        if (!session.ignoreErrors())
            throw new IOException(message);
        else
            System.err.println(message);
    }

    protected String getQuotedCqlBlob(String term)
    {
        return getQuotedCqlBlob(term.getBytes());
    }

    protected String getQuotedCqlBlob(byte[] term)
    {
        return String.format("'%s'", Hex.bytesToHex(term));
    }
}
