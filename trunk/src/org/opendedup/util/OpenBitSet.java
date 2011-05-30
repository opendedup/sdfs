package org.opendedup.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Arrays;
import java.io.Serializable;

/** An "open" BitSet implementation that allows direct access to the array of words
 * storing the bits.
 * <p/>
 * Unlike java.util.bitet, the fact that bits are packed into an array of longs
 * is part of the interface.  This allows efficient implementation of other algorithms
 * by someone other than the author.  It also allows one to efficiently implement
 * alternate serialization or interchange formats.
 * <p/>
 * <code>OpenBitSet</code> is faster than <code>java.util.BitSet</code> in most operations
 * and *much* faster at calculating cardinality of sets and results of set operations.
 * It can also handle sets of larger cardinality (up to 64 * 2**32-1)
 * <p/>
 * The goals of <code>OpenBitSet</code> are the fastest implementation possible, and
 * maximum code reuse.  Extra safety and encapsulation
 * may always be built on top, but if that's built in, the cost can never be removed (and
 * hence people re-implement their own version in order to get better performance).
 * If you want a "safe", totally encapsulated (and slower and limited) BitSet
 * class, use <code>java.util.BitSet</code>.
 * <p/>
 * <h3>Performance Results</h3>
 *
 Test system: Pentium 4, Sun Java 1.5_06 -server -Xbatch -Xmx64M
<br/>BitSet size = 1,000,000
<br/>Results are java.util.BitSet time divided by OpenBitSet time.
<table border="1">
 <tr>
  <th></th> <th>cardinality</th> <th>intersect_count</th> <th>union</th> <th>nextSetBit</th> <th>get</th> <th>iterator</th>
 </tr>
 <tr>
  <th>50% full</th> <td>3.36</td> <td>3.96</td> <td>1.44</td> <td>1.46</td> <td>1.99</td> <td>1.58</td>
 </tr>
 <tr>
   <th>1% full</th> <td>3.31</td> <td>3.90</td> <td>&nbsp;</td> <td>1.04</td> <td>&nbsp;</td> <td>0.99</td>
 </tr>
</table>
<br/>
Test system: AMD Opteron, 64 bit linux, Sun Java 1.5_06 -server -Xbatch -Xmx64M
<br/>BitSet size = 1,000,000
<br/>Results are java.util.BitSet time divided by OpenBitSet time.
<table border="1">
 <tr>
  <th></th> <th>cardinality</th> <th>intersect_count</th> <th>union</th> <th>nextSetBit</th> <th>get</th> <th>iterator</th>
 </tr>
 <tr>
  <th>50% full</th> <td>2.50</td> <td>3.50</td> <td>1.00</td> <td>1.03</td> <td>1.12</td> <td>1.25</td>
 </tr>
 <tr>
   <th>1% full</th> <td>2.51</td> <td>3.49</td> <td>&nbsp;</td> <td>1.00</td> <td>&nbsp;</td> <td>1.02</td>
 </tr>
</table>

 * @author yonik
 * @version $Id$
 */

public class OpenBitSet implements Cloneable, Serializable {
  protected long[] bits;
  protected int wlen;   // number of words (elements) used in the array

  /** Constructs an OpenBitSet large enough to hold numBits.
   *
   * @param numBits
   */
  public OpenBitSet(long numBits) {
    bits = new long[bits2words(numBits)];
    wlen = bits.length;
  }

  public OpenBitSet() {
    this(64);
  }

  /** Constructs an OpenBitSet from an existing long[].
   * <br/>
   * The first 64 bits are in long[0],
   * with bit index 0 at the least significant bit, and bit index 63 at the most significant.
   * Given a bit index,
   * the word containing it is long[index/64], and it is at bit number index%64 within that word.
   * <p>
   * numWords are the number of elements in the array that contain
   * set bits (non-zero longs).
   * numWords should be &lt= bits.length, and
   * any existing words in the array at position &gt= numWords should be zero.
   *
   */
  public OpenBitSet(long[] bits, int numWords) {
    this.bits = bits;
    this.wlen = numWords;
  }

  /** Returns the current capacity in bits (1 greater than the index of the last bit) */
  public long capacity() { return bits.length << 6; }

 /**
  * Returns the current capacity of this set.  Included for
  * compatibility.  This is *not* equal to {@link #cardinality}
  */
  public long size() {
	  return capacity();
  }

  /** Returns true if there are no set bits */
  public boolean isEmpty() { return cardinality()==0; }

  /** Expert: returns the long[] storing the bits */
  public long[] getBits() { return bits; }

  /** Expert: sets a new long[] to use as the bit storage */
  public void setBits(long[] bits) { this.bits = bits; }

  /** Expert: gets the number of longs in the array that are in use */
  public int getNumWords() { return wlen; }

  /** Expert: sets the number of longs in the array that are in use */
  public void setNumWords(int nWords) { this.wlen=nWords; }



  /** Returns true or false for the specified bit index. */
  public boolean get(int index) {
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    if (i>=bits.length) return false;

    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    return (bits[i] & bitmask) != 0;
  }


 /** Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size
   */
  public boolean fastGet(int index) {
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    return (bits[i] & bitmask) != 0;
  }



 /** Returns true or false for the specified bit index
  * The index should be less than the OpenBitSet size
  */
  public boolean get(long index) {
    int i = (int)(index >> 6);             // div 64
    if (i>=bits.length) return false;
    int bit = (int)index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    return (bits[i] & bitmask) != 0;
  }

  /** Returns true or false for the specified bit index.  Allows specifying
   * an index outside the current size. */
  public boolean fastGet(long index) {
    int i = (int)(index >> 6);               // div 64
    int bit = (int)index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    return (bits[i] & bitmask) != 0;
  }

  /*
  // alternate implementation of get()
  public boolean get1(int index) {
    int i = index >> 6;                // div 64
    int bit = index & 0x3f;            // mod 64
    return ((bits[i]>>>bit) & 0x01) != 0;
    // this does a long shift and a bittest (on x86) vs
    // a long shift, and a long AND, (the test for zero is prob a no-op)
    // testing on a P4 indicates this is slower than (bits[i] & bitmask) != 0;
  }
  */


  /** returns 1 if the bit is set, 0 if not.
   * The index should be less than the OpenBitSet size
   */
  public int getBit(int index) {
    int i = index >> 6;                // div 64
    int bit = index & 0x3f;            // mod 64
    return ((int)(bits[i]>>>bit)) & 0x01;
  }


  /*
  public boolean get2(int index) {
    int word = index >> 6;            // div 64
    int bit = index & 0x0000003f;     // mod 64
    return (bits[word] << bit) < 0;   // hmmm, this would work if bit order were reversed
    // we could right shift and check for parity bit, if it was available to us.
  }
  */

  /** sets a bit, expanding the set size if necessary */
  public void set(long index) {
    int wordNum = expandingWordNum(index);
    int bit = (int)index & 0x3f;
    long bitmask = 1L << bit;
    bits[wordNum] |= bitmask;
  }


 /** Sets the bit at the specified index.
  * The index should be less than the OpenBitSet size.
  */
  public void fastSet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] |= bitmask;
  }

 /** Sets the bit at the specified index.
  * The index should be less than the OpenBitSet size.
  */
  public void fastSet(long index) {
    int wordNum = (int)(index >> 6);
    int bit = (int)index & 0x3f;
    long bitmask = 1L << bit;
    bits[wordNum] |= bitmask;
  }

  protected int expandingWordNum(long index) {
    int wordNum = (int)(index >> 6);
    if (wordNum>=wlen) {
      ensureCapacity(index+1);
      wlen = wordNum+1;
    }
    return wordNum;
  }


  /** clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void fastClear(int index) {
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;
    bits[wordNum] &= ~bitmask;
    // hmmm, it takes one more instruction to clear than it does to set... any
    // way to work around this?  If there were only 63 bits per word, we could
    // use a right shift of 10111111...111 in binary to position the 0 in the
    // correct place (using sign extension).
    // Could also use Long.rotateRight() or rotateLeft() *if* they were converted
    // by the JVM into a native instruction.
    // bits[word] &= Long.rotateLeft(0xfffffffe,bit);
  }

  /** clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void fastClear(long index) {
    int wordNum = (int)(index >> 6); // div 64
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] &= ~bitmask;
  }

  /** clears a bit, allowing access beyond the current set size */
  public void clear(long index) {
    int wordNum = (int)(index >> 6); // div 64
    if (wordNum>=wlen) return;
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] &= ~bitmask;
  }

  /** Sets a bit and returns the previous value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean getAndSet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] |= bitmask;
    return val;
  }

  /** Sets a bit and returns the previous value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean getAndSet(long index) {
    int wordNum = (int)(index >> 6);      // div 64
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum] & bitmask) != 0;
    bits[wordNum] |= bitmask;
    return val;
  }

  /** flips a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void fastFlip(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] ^= bitmask;
  }

  /** flips a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void fastFlip(long index) {
    int wordNum = (int)(index >> 6);   // div 64
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] ^= bitmask;
  }

  /** flips a bit, expanding the set size if necessary */
  public void flip(long index) {
    int wordNum = expandingWordNum(index);
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] ^= bitmask;
  }

  /** flips a bit and returns the resulting bit value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean flipAndGet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] ^= bitmask;
    return (bits[wordNum] & bitmask) != 0;
  }

  /** flips a bit and returns the resulting bit value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean flipAndGet(long index) {
    int wordNum = (int)(index >> 6);   // div 64
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    bits[wordNum] ^= bitmask;
    return (bits[wordNum] & bitmask) != 0;
  }

  /** Flips a range of bits, expanding the set size if necessary
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to flip
   */
  public void flip(long startIndex, long endIndex) {
    if (endIndex <= startIndex) return;

    int oldlen = wlen;
    ensureCapacity(endIndex);
    int startWord = (int)(startIndex>>6);
    int endWord   = (int)(endIndex>>6);

    /*** Grrr, java shifting wraps around so -1L>>64 == -1
    long startmask = -1L << (startIndex & 0x3f);     // example: 11111...111000
    long endmask = -1L >>> (64-(endIndex & 0x3f));   // example: 00111...111111
    ***/

    long startmask = -1L << startIndex;
    long endmask = (endIndex&0x3f)==0 ? 0 : -1L >>> (64-endIndex);

    if (this.wlen <= endWord) {
      this.wlen = endWord;
      if (endmask!=0) this.wlen++;
    }

    if (startWord == endWord) {
      bits[startWord] ^= (startmask & endmask);
      return;
    }

    bits[startWord] ^= startmask;

    int middle = Math.min(oldlen,endWord);
    for (int i=startWord+1; i<middle; i++) {
      bits[i] = ~bits[i];
    }

    if (endWord>middle) {
      Arrays.fill(bits,middle,endWord,-1L);
    }

    if (endmask!=0) {
      bits[endWord] ^= endmask;
    }
  }


  /*
  public static int pop(long v0, long v1, long v2, long v3) {
    // derived from pop_array by setting last four elems to 0.
    // exchanges one pop() call for 10 elementary operations
    // saving about 7 instructions... is there a better way?
      long twosA=v0 & v1;
      long ones=v0^v1;

      long u2=ones^v2;
      long twosB =(ones&v2)|(u2&v3);
      ones=u2^v3;

      long fours=(twosA&twosB);
      long twos=twosA^twosB;

      return (pop(fours)<<2)
             + (pop(twos)<<1)
             + pop(ones);

  }
  */


  /** @return the number of set bits */
  public long cardinality() {
    return BitUtil.pop_array(bits,0,wlen);
  }

 /** Returns the popcount or cardinality of the intersection of the two sets.
   * Neither set is modified.
   */
  public static long intersectionCount(OpenBitSet a, OpenBitSet b) {
    return BitUtil.pop_intersect(a.bits, b.bits, 0, Math.min(a.wlen, b.wlen));
 }

  /** Returns the popcount or cardinality of the union of the two sets.
    * Neither set is modified.
    */
  public static long unionCount(OpenBitSet a, OpenBitSet b) {
    long tot = BitUtil.pop_union(a.bits, b.bits, 0, Math.min(a.wlen, b.wlen));
    if (a.wlen < b.wlen) {
      tot += BitUtil.pop_array(b.bits, a.wlen, b.wlen-a.wlen);
    } else if (a.wlen > b.wlen) {
      tot += BitUtil.pop_array(a.bits, b.wlen, a.wlen-b.wlen);
    }
    return tot;
  }

  /** Returns the popcount or cardinality of "a and not b"
   * or "intersection(a, not(b))".
   * Neither set is modified.
   */
  public static long andNotCount(OpenBitSet a, OpenBitSet b) {
    long tot = BitUtil.pop_andnot(a.bits, b.bits, 0, Math.min(a.wlen, b.wlen));
    if (a.wlen > b.wlen) {
      tot += BitUtil.pop_array(a.bits, b.wlen, a.wlen-b.wlen);
    }
    return tot;
  }

 /** Returns the popcount or cardinality of the exclusive-or of the two sets.
  * Neither set is modified.
  */
  public static long xorCount(OpenBitSet a, OpenBitSet b) {
    long tot = BitUtil.pop_xor(a.bits, b.bits, 0, Math.min(a.wlen, b.wlen));
    if (a.wlen < b.wlen) {
      tot += BitUtil.pop_array(b.bits, a.wlen, b.wlen-a.wlen);
    } else if (a.wlen > b.wlen) {
      tot += BitUtil.pop_array(a.bits, b.wlen, a.wlen-b.wlen);
    }
    return tot;
  }


  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int nextSetBit(int index) {
    int i = index>>6;
    if (i>=wlen) return -1;
    int subIndex = index & 0x3f;      // index within the word
    long word = bits[i] >> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (i<<6) + subIndex + BitUtil.ntz(word);
    }

    while(++i < wlen) {
      word = bits[i];
      if (word!=0) return (i<<6) + BitUtil.ntz(word);
    }

    return -1;
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public long nextSetBit(long index) {
    int i = (int)(index>>>6);
    if (i>=wlen) return -1;
    int subIndex = (int)index & 0x3f; // index within the word
    long word = bits[i] >>> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (((long)i)<<6) + (subIndex + BitUtil.ntz(word));
    }

    while(++i < wlen) {
      word = bits[i];
      if (word!=0) return (((long)i)<<6) + BitUtil.ntz(word);
    }

    return -1;
  }




  public Object clone() {
    try {
      OpenBitSet obs = (OpenBitSet)super.clone();
      obs.bits = obs.bits.clone();  // hopefully an array clone is as fast(er) than arraycopy
      return obs;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  /** this = this AND other */
  public void intersect(OpenBitSet other) {
    int newLen= Math.min(this.wlen,other.wlen);
    long[] thisArr = this.bits;
    long[] otherArr = other.bits;
    // testing against zero can be more efficient
    int pos=newLen;
    while(--pos>=0) {
      thisArr[pos] &= otherArr[pos];
    }
    if (this.wlen > newLen) {
      // fill zeros from the new shorter length to the old length
      Arrays.fill(bits,newLen,this.wlen,0);
    }
    this.wlen = newLen;
  }

  /** this = this OR other */
  public void union(OpenBitSet other) {
    int newLen = Math.max(wlen,other.wlen);
    ensureCapacityWords(newLen);

    long[] thisArr = this.bits;
    long[] otherArr = other.bits;
    int pos=Math.min(wlen,other.wlen);
    while(--pos>=0) {
      thisArr[pos] |= otherArr[pos];
    }
    if (this.wlen < newLen) {
      System.arraycopy(otherArr, this.wlen, thisArr, this.wlen, newLen-this.wlen);
    }
    this.wlen = newLen;
  }


  /** Remove all elements set in other. this = this AND_NOT other */
  public void remove(OpenBitSet other) {
    int idx = Math.min(wlen,other.wlen);
    long[] thisArr = this.bits;
    long[] otherArr = other.bits;
    while(--idx>=0) {
      thisArr[idx] &= ~otherArr[idx];
    }
  }

  /** this = this XOR other */
  public void xor(OpenBitSet other) {
    int newLen = Math.max(wlen,other.wlen);
    ensureCapacityWords(newLen);

    long[] thisArr = this.bits;
    long[] otherArr = other.bits;
    int pos=Math.min(wlen,other.wlen);
    while(--pos>=0) {
      thisArr[pos] ^= otherArr[pos];
    }
    if (this.wlen < newLen) {
      System.arraycopy(otherArr, this.wlen, thisArr, this.wlen, newLen-this.wlen);
    }
    this.wlen = newLen;
  }


  // some BitSet compatability methods

  //** see {@link intersect} */
  public void and(OpenBitSet other) {
    intersect(other);
  }

  //** see {@link union} */
  public void or(OpenBitSet other) {
    union(other);
  }

  //** see {@link andNot} */
  public void andNot(OpenBitSet other) {
    remove(other);
  }

  /** returns true if the sets have any elements in common */
  public boolean intersects(OpenBitSet other) {
    int pos = Math.min(this.wlen, other.wlen);
    long[] thisArr = this.bits;
    long[] otherArr = other.bits;
    while (--pos>=0) {
      if ((thisArr[pos] & otherArr[pos])!=0) return true;
    }
    return false;
  }



  /** Expand the long[] with the size given as a number of words (64 bit longs).
   * getNumWords() is unchanged by this call.
   */
  public void ensureCapacityWords(int numWords) {
    if (bits.length < numWords) {
      long[] newBits = new long[numWords];
      System.arraycopy(bits,0,newBits,0,wlen);
      bits = newBits;
    }
  }

  /** Ensure that the long[] is big enough to hold numBits, expanding it if necessary.
   * getNumWords() is unchanged by this call.
   */
  public void ensureCapacity(long numBits) {
    ensureCapacityWords(bits2words(numBits));
  }

  /** Lowers numWords, the number of words in use,
   * by checking for trailing zero words.
   */
  public void trimTrailingZeros() {
    int idx = wlen-1;
    while (idx>=0 && bits[idx]==0) idx--;
    wlen = idx+1;
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(long numBits) {
   return (int)(((numBits-1)>>>6)+1);
  }


  /** returns true if both sets have the same bits set */
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OpenBitSet)) return false;
    OpenBitSet a;
    OpenBitSet b = (OpenBitSet)o;
    // make a the larger set.
    if (b.wlen > this.wlen) {
      a = b; b=this;
    } else {
      a=this;
    }

    // check for any set bits out of the range of b
    for (int i=a.wlen-1; i>=b.wlen; i--) {
      if (a.bits[i]!=0) return false;
    }

    for (int i=b.wlen-1; i>=0; i--) {
      if (a.bits[i] != b.bits[i]) return false;
    }

    return true;
  }


  public int hashCode() {
	  long h = 0x98761234;  // something non-zero for length==0
	  for (int i = bits.length; --i>=0;) {
      h ^= bits[i];
      h = (h << 1) | (h >>> 31); // rotate left
    }
    return (int)((h>>32) ^ h);  // fold leftmost bits into right
  }

}

