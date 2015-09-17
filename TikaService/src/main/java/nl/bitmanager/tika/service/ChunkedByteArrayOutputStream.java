/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package nl.bitmanager.tika.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

/**
 * ByteArrayOutputStream that does not create 1 monolitic array but a list of arrays instead.
 * Also supports a maximum array size. If the array grows more than this value an exception is thrown.
 */
public class ChunkedByteArrayOutputStream extends OutputStream {
    private ArrayList<byte[]> buffers;
    private byte[] curBuffer;
    private int curBufferCount;
    private int chunkSize;
    private int maxSize;
    
    public ChunkedByteArrayOutputStream (int chunkSize) {
        this.chunkSize = chunkSize < 2048 ? 2048 : chunkSize;
        curBuffer = new byte[this.chunkSize];
        maxSize = Integer.MAX_VALUE;
    }
    
    public void setMaxSize (String ms) {
    	if (ms==null || ms.length()==0) {
    		maxSize = Integer.MAX_VALUE;
    		return;
        }
    	maxSize = parseSize(ms);
    }
    
    private static int parseSize(String x) {
    	x = x.toLowerCase();
    	if (x.endsWith("k")) return 1024 * sizeToInt (x, 1);
    	if (x.endsWith("m")) return 1024 * 1024 * sizeToInt (x, 1);
    	if (x.endsWith("kb")) return 1024 * sizeToInt (x, 2);
    	if (x.endsWith("mb")) return 1024 * 1024 * sizeToInt (x, 2);
    	return Integer.parseInt (x);
    }
    private static int sizeToInt (String x, int suffixLen) {
    	return Integer.parseInt(x.substring(0, x.length() - suffixLen));
    }
    
    
    public int getLength() {
        int len = curBufferCount;
        if (buffers != null) len += buffers.size() * chunkSize;
        return len;
    }
    
    @Override
    public void write(int b) throws IOException {
        if (curBufferCount >= chunkSize)
            newBuffer();
        curBuffer[curBufferCount] = (byte)b;
        ++curBufferCount;
    }
    
    @Override
    public void write(byte b[], int off, int len) {
        if (len<=0) return;
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }
        
        while (true) {
            int free = chunkSize - curBufferCount;
            if (free <= 0) {
                newBuffer();
                free = chunkSize;
            }
            
            if (free > len) free = len;
            System.arraycopy(b,  off, curBuffer, curBufferCount, free);
            curBufferCount += free;
            if (len==free) break;
            len -= free;
            off += free;
        }
    }
    
    public void reset() {
        buffers = null;
        curBufferCount = 0;
    }
    
    public void writeTo(OutputStream out) throws IOException {
        if (buffers != null) {
            for (byte[] b : buffers)
                out.write(b);
        }
        if (curBufferCount>0)
            out.write(curBuffer, 0, curBufferCount);
    }
    
    private void newBuffer() {
        if (buffers==null) buffers = new ArrayList<byte[]>();
        else if (getLength() >= this.maxSize) throw new RuntimeException ("Max. buffersize exceeded: " + getLength() + ".");
        buffers.add(curBuffer);
        curBuffer = null;
        curBuffer = new byte[this.chunkSize];
        curBufferCount = 0;
    }
}
