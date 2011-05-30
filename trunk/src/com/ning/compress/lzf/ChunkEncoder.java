/* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package com.ning.compress.lzf;

/**
 * Class that handles actual encoding of individual chunks.
 * Resulting chunks can be compressed or non-compressed; compression
 * is only used if it actually reduces chunk size (including overhead
 * of additional header bytes)
 * 
 * @author tatu@ning.com
 */
public class ChunkEncoder
{
    // Beyond certain point we won't be able to compress; let's use 16 bytes as cut-off
    private static final int MIN_BLOCK_TO_COMPRESS = 16;

    private static final int MIN_HASH_SIZE = 256;

    // Not much point in bigger tables, with 8k window
    private static final int MAX_HASH_SIZE = 16384;

    private static final int MAX_OFF = 1 << 13; // 8k
    private static final int MAX_REF = (1 << 8) + (1 << 3); // 264
        
    // // Encoding tables

    /**
     * Buffer in which encoded content is stored during processing
     */
    private final byte[] _encodeBuffer;    
    
    private final int[] _hashTable;
    
    private final int _hashModulo;

    /**
     * @param totalLength Total encoded length; used for calculating size
     *   of hash table to use
     */
    public ChunkEncoder(int totalLength)
    {
        int largestChunkLen = Math.max(totalLength, LZFChunk.MAX_CHUNK_LEN);
        
        int hashLen = calcHashLen(largestChunkLen);
        _hashTable = new int[hashLen];
        _hashModulo = hashLen-1;
        // Ok, then, what's the worst case output buffer length?
        // length indicator for each 32 literals, so:
        int bufferLen = largestChunkLen + ((largestChunkLen + 31) >> 5);
        _encodeBuffer = new byte[bufferLen];
    }

    /**
     * Method for compressing (or not) individual chunks
     */
    public LZFChunk encodeChunk(byte[] data, int offset, int len)
    {
        if (len >= MIN_BLOCK_TO_COMPRESS) {
            /* If we have non-trivial block, and can compress it by at least
             * 2 bytes (since header is 2 bytes longer), let's compress:
             */
            int compLen = tryCompress(data, offset, offset+len, _encodeBuffer, 0);
            if (compLen < (len-2)) { // nah; just return uncompressed
                return LZFChunk.createCompressed(len, _encodeBuffer, 0, compLen);
            }
        }
        // Otherwise leave uncompressed:
        return LZFChunk.createNonCompressed(data, offset, len);
    }
    
    private static int calcHashLen(int chunkSize)
    {
        // in general try get hash table size of 2x input size
        chunkSize += chunkSize;
        // but no larger than max size:
        if (chunkSize >= MAX_HASH_SIZE) {
            return MAX_HASH_SIZE;
        }
        // otherwise just need to round up to nearest 2x
        int hashLen = MIN_HASH_SIZE;
        while (hashLen < chunkSize) {
            hashLen += hashLen;
        }
        return hashLen;
    }
    
    private int first(byte[] in, int inPos) {
        return (in[inPos] << 8) + (in[inPos + 1] & 255);
    }

    private static int next(int v, byte[] in, int inPos) {
        return (v << 8) + (in[inPos + 2] & 255);
    }


    private int hash(int h) {
        // or 184117; but this seems to give better hashing?
        return ((h * 57321) >> 9) & _hashModulo;
        // original lzf-c.c used this:
        //return (((h ^ (h << 5)) >> (24 - HLOG) - h*5) & _hashModulo;
        // but that didn't seem to provide better matches
    }
    
    private int tryCompress(byte[] in, int inPos, int inEnd, byte[] out, int outPos)
    {
        int literals = 0;
        outPos++;
        int hash = first(in, 0);
        inEnd -= 4;
        final int firstPos = inPos; // so that we won't have back references across block boundary
        while (inPos < inEnd) {
            byte p2 = in[inPos + 2];
            // next
            hash = (hash << 8) + (p2 & 255);
            int off = hash(hash);
            int ref = _hashTable[off];
            _hashTable[off] = inPos;
            if (ref < inPos
                        && ref >= firstPos
                        && (off = inPos - ref - 1) < MAX_OFF
                        && in[ref + 2] == p2
                        && in[ref + 1] == (byte) (hash >> 8)
                        && in[ref] == (byte) (hash >> 16)) {
                // match
                int maxLen = inEnd - inPos + 2;
                if (maxLen > MAX_REF) {
                    maxLen = MAX_REF;
                }
                if (literals == 0) {
                    outPos--;
                } else {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                }
                int len = 3;
                while (len < maxLen && in[ref + len] == in[inPos + len]) {
                    len++;
                }
                len -= 2;
                if (len < 7) {
                    out[outPos++] = (byte) ((off >> 8) + (len << 5));
                } else {
                    out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                    out[outPos++] = (byte) (len - 7);
                }
                out[outPos++] = (byte) off;
                outPos++;
                inPos += len;
                hash = first(in, inPos);
                hash = next(hash, in, inPos);
                _hashTable[hash(hash)] = inPos++;
                hash = next(hash, in, inPos);
                _hashTable[hash(hash)] = inPos++;
            } else {
                out[outPos++] = in[inPos++];
                literals++;
                if (literals == LZFChunk.MAX_LITERAL) {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                    outPos++;
                }
            }
        }
        inEnd += 4;
        while (inPos < inEnd) {
            out[outPos++] = in[inPos++];
            literals++;
            if (literals == LZFChunk.MAX_LITERAL) {
                out[outPos - literals - 1] = (byte) (literals - 1);
                literals = 0;
                outPos++;
            }
        }
        out[outPos - literals - 1] = (byte) (literals - 1);
        if (literals == 0) {
            outPos--;
        }
        return outPos;
    }

}
