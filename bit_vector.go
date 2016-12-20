/**
 * Copyright 2016 l0vest0rm.hll authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"): you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http: *www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

// Created by xuning on 2016/12/19

package hll

const(
    // rather than doing division to determine how a bit index fits into 64bit
    // words (i.e. longs), bit shifting is used
    LOG2_BITS_PER_WORD = 6/*=>64bits*/
    BITS_PER_WORD = 1 << LOG2_BITS_PER_WORD
    BITS_PER_WORD_MASK = BITS_PER_WORD - 1

    // ditto from above but for bytes (for output)
    LOG2_BITS_PER_BYTE = 3/*=>8bits*/
    BITS_PER_BYTE = 1 << LOG2_BITS_PER_BYTE

    BYTES_PER_WORD = 8/*8 bytes in a long*/
)

type BitVector struct {
    // 64bit words
    words []uint64

    // the width of a register in bits (this cannot be more than 64 (the word size))
    registerWidth uint64
    count uint
    registerMask uint64
}

/**
     * @param  width the width of each register.  This cannot be negative or
     *         zero or greater than 63 (the signed word size).
     * @param  count the number of registers.  This cannot be negative or zero
     */
func NewBitVector(width uint, count uint) *BitVector {
    this := &BitVector{}
    // ceil((width * count)/BITS_PER_WORD)
    this.words = make([]uint64, ((width * count) + BITS_PER_WORD_MASK) >> LOG2_BITS_PER_WORD, ((width * count) + BITS_PER_WORD_MASK) >> LOG2_BITS_PER_WORD)
    //this.words = [((width * count) + BITS_PER_WORD_MASK) >> LOG2_BITS_PER_WORD]uint64{}
    this.registerWidth = uint64(width)
    this.count = count

    this.registerMask = (1 << width) - 1

    return this
}

/**
     * Sets the value of the specified index register if and only if the specified
     * value is greater than the current value in the register.  This is equivalent
     * to but much more performant than:<p/>
     *
     * <pre>vector.setRegister(index, Math.max(vector.getRegister(index), value));</pre>
     *
     * @param  registerIndex the index of the register whose value is to be set.
     *         This cannot be negative
     * @param  value the value to set in the register if and only if this value
     *         is greater than the current value in the register
     * @return <code>true</code> if and only if the specified value is greater
     *         than or equal to the current register value.  <code>false</code>
     *         otherwise.
     * @see #getRegister(long)
     * @see #setRegister(long, long)
     * @see java.lang.Math#max(long, long)
     */
// NOTE:  if this changes then setRegister() must change
func (this *BitVector)setMaxRegister(registerIndex uint64, value uint64) bool {
    bitIndex := registerIndex * this.registerWidth
    firstWordIndex := bitIndex >> LOG2_BITS_PER_WORD/*aka (bitIndex / BITS_PER_WORD)*/
    secondWordIndex := (bitIndex + this.registerWidth - 1) >> LOG2_BITS_PER_WORD/*see above*/
    bitRemainder := bitIndex & BITS_PER_WORD_MASK/*aka (bitIndex % BITS_PER_WORD)*/

    // NOTE:  matches getRegister()
    var registerValue uint64
    words := this.words/*for convenience/performance*/;
    if firstWordIndex == secondWordIndex{
        registerValue = ((words[firstWordIndex] >> bitRemainder) & this.registerMask)
    }else {
        /*register spans words*/
        /*no need to mask since at top of word*/
        registerValue = (words[firstWordIndex] >> bitRemainder)| (words[secondWordIndex] << (BITS_PER_WORD - bitRemainder)) & this.registerMask
    }

    // determine which is the larger and update as necessary
    if(value > registerValue) {
    // NOTE:  matches setRegister()
    if(firstWordIndex == secondWordIndex) {
    // clear then set
    words[firstWordIndex] &= ^(this.registerMask << bitRemainder)
    words[firstWordIndex] |= (value << bitRemainder)
    } else {/*register spans words*/
    // clear then set each partial word
    words[firstWordIndex] &= (1 << bitRemainder) - 1
    words[firstWordIndex] |= (value << bitRemainder)

    words[secondWordIndex] &= ^(this.registerMask >> (BITS_PER_WORD - bitRemainder))
    words[secondWordIndex] |= (value >> (BITS_PER_WORD - bitRemainder))
    }
    } /* else -- the register value is greater (or equal) so nothing needs to be done */

    return (value >= registerValue)
}

/**
     * Creates a deep copy of this vector.
     *
     * @see java.lang.Object#clone()
     */
func (this *BitVector) Clone() *BitVector {
    c := NewBitVector(uint(this.registerWidth), this.count)
    copy(c.words, this.words)
    return c
}

/**
     * @param  registerIndex the index of the register whose value is to be
     *         retrieved.  This cannot be negative.
     * @return the value at the specified register index
     * @see #setRegister(long, long)
     * @see #setMaxRegister(long, long)
     */
// NOTE:  if this changes then setMaxRegister() must change
func (this *BitVector) getRegister(registerIndex uint64) uint64 {
    bitIndex := registerIndex * this.registerWidth;
    firstWordIndex := (bitIndex >> LOG2_BITS_PER_WORD)/*aka (bitIndex / BITS_PER_WORD)*/;
    secondWordIndex := ((bitIndex + this.registerWidth - 1) >> LOG2_BITS_PER_WORD)/*see above*/;
    bitRemainder := (bitIndex & BITS_PER_WORD_MASK)/*aka (bitIndex % BITS_PER_WORD)*/;

    if(firstWordIndex == secondWordIndex){
        return ((this.words[firstWordIndex] >> bitRemainder) & this.registerMask);
    }

    /* else -- register spans words */
    return (this.words[firstWordIndex] >> bitRemainder) | (this.words[secondWordIndex] << (BITS_PER_WORD - bitRemainder)) & this.registerMask
}

/**
     * @return a <code>LongIterator</code> for iterating starting at the register
     *         with index zero. This will never be <code>null</code>.
     */
func (this *BitVector)sum() (float64, int) {
    registerWidth := this.registerWidth;
    words := this.words;
    registerMask := this.registerMask;

    // register setup
    wordIndex := 0
    remainingWordBits := uint64(BITS_PER_WORD)
    word := words[wordIndex];

    // compute the "indicator function" -- sum(2^(-M[j])) where M[j] is the
    // 'j'th register value
    sum := float64(0)
    numberOfZeroes := 0/*"V" in the paper*/
    var register uint64
    for registerIndex := uint(0);registerIndex < this.count; registerIndex +=1{
        if(remainingWordBits >= registerWidth) {
            register = word & registerMask;

            // shift to the next register
            word >>= registerWidth;
            remainingWordBits -= registerWidth;
        } else { /*insufficient bits remaining in current word*/
            wordIndex++/*move to the next word*/;

            register = (word | (words[wordIndex] << remainingWordBits)) & registerMask;

            // shift to the next partial register (word)
            word = words[wordIndex] >> (registerWidth - remainingWordBits);
            remainingWordBits += BITS_PER_WORD - registerWidth;
        }
        sum += 1.0 / float64(uint64(1) << register)
        if(register == 0){
            numberOfZeroes += 1
        }
    }

    return sum, numberOfZeroes
}