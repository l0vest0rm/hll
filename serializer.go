//it is ported from [java-hll](https://github.com/aggregateknowledge/java-hll) and it is storage-compatible with the java version,thanks to the original author.
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

// Created by xuning on 2016/12/20

package hll

import(
    "errors"
    "fmt"
)

type bigEndianAscendingWordSerializer struct {
    // The length in bits of the words to be written.
    wordLength uint
    // The number of words to be written.
    wordCount uint

    // The byte array to which the words are serialized.
    bytes []byte

    // ------------------------------------------------------------------------
    // Write state
    // Number of bits that remain writable in the current byte.
    bitsLeftInByte uint
    // Index of byte currently being written to.
    byteIndex uint
    // Number of words written.
    wordsWritten uint
}

func newBigEndianAscendingWordSerializer(wordLength uint, wordCount uint) *bigEndianAscendingWordSerializer {
    return newBigEndianAscendingWordSerializer2(wordLength, wordCount, HEADER_BYTE_COUNT)
}

// ========================================================================
/**
 * @param wordLength the length in bits of the words to be serialized. Must
 *        be greater than or equal to 1 and less than or equal to 64.
 * @param wordCount the number of words to be serialized. Must be greater than
 *        or equal to zero.
 * @param bytePadding the number of leading bytes that should pad the
 *        serialized words. Must be greater than or equal to zero.
 */
func newBigEndianAscendingWordSerializer2(wordLength uint, wordCount uint, bytePadding uint) *bigEndianAscendingWordSerializer {
    if((wordLength < 1) || (wordLength > BITS_PER_LONG)) {
        panic(fmt.Errorf("Word length must be >= 1 and <= 64. (was: %d)" ,wordLength))
    }

    this := &bigEndianAscendingWordSerializer{}
    this.wordLength = wordLength;
    this.wordCount = wordCount;

    bitsRequired := (wordLength * wordCount);
    leftoverBits := ((bitsRequired % BITS_PER_BYTE) != 0);
    var bytesRequired uint
    if leftoverBits {
        bytesRequired = (bitsRequired / BITS_PER_BYTE) + 1 + bytePadding
    }else {
        bytesRequired = (bitsRequired / BITS_PER_BYTE) + bytePadding
    }

    this.bytes = make([]byte, bytesRequired)
    this.bitsLeftInByte = BITS_PER_BYTE;
    this.byteIndex = bytePadding;
    this.wordsWritten = 0;

    return this
}

func (this *bigEndianAscendingWordSerializer)writeWord(word uint64) error {
    if(this.wordsWritten == this.wordCount) {
        return errors.New("Cannot write more words, backing array full!");
    }

    bitsLeftInWord := this.wordLength;

    for ; bitsLeftInWord > 0; {
        // Move to the next byte if the current one is fully packed.
        if this.bitsLeftInByte == 0 {
            this.byteIndex++;
            this.bitsLeftInByte = BITS_PER_BYTE;
        }

        var consumedMask uint64
        if(bitsLeftInWord == 64) {
            consumedMask = 0xffffffffffffffff
        } else {
            consumedMask = ((1 << bitsLeftInWord) - 1);
        }

        // Fix how many bits will be written in this cycle. Choose the
        // smaller of the remaining bits in the word or byte.
        var numberOfBitsToWrite uint
        if this.bitsLeftInByte < bitsLeftInWord {
            numberOfBitsToWrite = this.bitsLeftInByte
        } else {
            numberOfBitsToWrite = bitsLeftInWord
        }
        bitsInByteRemainingAfterWrite := (this.bitsLeftInByte - numberOfBitsToWrite);

        // In general, we write the highest bits of the word first, so we
        // strip the highest bits that were consumed in previous cycles.
        remainingBitsOfWordToWrite := (word & consumedMask);

        var bitsThatTheByteCanAccept uint64
        // If there is more left in the word than can be written to this
        // byte, shift off the bits that can't be written off the bottom.
        if(bitsLeftInWord > numberOfBitsToWrite) {
            bitsThatTheByteCanAccept = (remainingBitsOfWordToWrite >> (bitsLeftInWord - this.bitsLeftInByte));
        } else {
        // If the byte can accept all remaining bits, there is no need
        // to shift off the bits that won't be written in this cycle.
            bitsThatTheByteCanAccept = remainingBitsOfWordToWrite;
        }

        // Align the word bits to write up against the byte bits that have
        // already been written. This shift may do nothing if the remainder
        // of the byte is being consumed in this cycle.
        alignedBits := (bitsThatTheByteCanAccept << bitsInByteRemainingAfterWrite);

        // Update the byte with the alignedBits.
        this.bytes[this.byteIndex] |= byte(alignedBits);

        // Update state with bit count written.
        bitsLeftInWord -= numberOfBitsToWrite;
        this.bitsLeftInByte = bitsInByteRemainingAfterWrite;
    }

    this.wordsWritten ++;
    return nil
}

func (this *bigEndianAscendingWordSerializer) getBytes() []byte {
    if(this.wordsWritten < this.wordCount) {
        panic(fmt.Sprintf("Not all words have been written! (%d/%d)", this.wordsWritten, this.wordCount));
    }

    return this.bytes
}
