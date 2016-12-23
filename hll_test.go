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

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

func randClientids(count int) []uint64 {
	clientids := make([]uint64, count)
	for i := 0; i < count; i++ {
		clientid := uint64(rand.Int63())
		clientids = append(clientids, clientid)
	}
	return clientids
}

func TestUnion(t *testing.T) {
	//count := 40000000
	count := 10

	clientids := randClientids(count)

	h := hyperloglog(clientids)
	data := h.ToBytes()
	fmt.Printf("bslen:%d\n", len(data))

	h2, _ := NewHll(15, 5)
	h2.Union(h)

	fmt.Printf("accuracy:%f\n", float64(h2.Cardinality())/float64(count))
}

func hyperloglog(clientids []uint64) *Hll {
	fmt.Printf("clientids:%d\n", len(clientids))
	t1 := time.Now().UnixNano()

	hllType := -1
	h, _ := NewHll(16, 5)
	for i, clientid := range clientids {
		if h.hllType != hllType {
			fmt.Printf("index:%d,hllType:%d\n", i, h.hllType)
			hllType = h.hllType
		}
		h.Add(clientid)
	}

	t2 := time.Now().UnixNano()
	fmt.Printf("time:%d,accuracy:%f\n", t2-t1, float64(h.Cardinality())/float64(len(clientids)))
	return h
}

func TestHyperloglog(t *testing.T) {
	//count := 40000000
	count := 6000

	clientids := randClientids(count)

	h := hyperloglog(clientids)
	data := h.ToBytes()
	fmt.Printf("bslen:%d\n", len(data))

	filename := "/tmp/hyperloglog.dat"
	err := ioutil.WriteFile(filename, data, os.ModePerm)
	if err != nil {
		fmt.Printf("WriteFile fail,err:%s\n", err.Error())
	}

	h2, err := NewHllFromBytes(data)
	if err != nil {
		fmt.Printf("NewHllFromBytes,err:%s\n", err.Error())
	}

	clientids2 := randClientids(count)
	h3 := hyperloglog(clientids2)

	h.Union(h2)
	h.Union(h3)

	fmt.Printf("accuracy:%f\n", float64(h.Cardinality())/float64(count))
}

func TestHyperloglogParams(t *testing.T) {
	h, _ := NewHll(16, 5)
	fmt.Printf("explicitThreshold:%d\n", h.explicitThreshold)
	fmt.Printf("sparseThreshold:%d\n", h.sparseThreshold)
	fmt.Printf("shortWordLength:%d\n", h.shortWordLength)
}
