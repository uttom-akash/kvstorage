package core

import (
	"github.com/devopsfaith/bloomfilter"
	baseBloomfilter "github.com/devopsfaith/bloomfilter/bloomfilter"
)

type BloomFilter struct {
	bloomFilter *baseBloomfilter.Bloomfilter
	config      *BloomFilterConfig
}

type BloomFilterConfig struct {
	N        uint
	P        float64
	HashName string
}

func NewBloomFilterConfig(n uint, p float64, hashName string) *BloomFilterConfig {
	return &BloomFilterConfig{
		n, p, hashName,
	}
}

func NewBloomFilter(n uint, p float64, hashName string) *BloomFilter {
	config := NewBloomFilterConfig(n, p, hashName)

	return &BloomFilter{
		bloomFilter: baseBloomfilter.New(bloomfilter.Config{
			N:        config.N,
			P:        config.P,
			HashName: config.HashName,
		}),
		config: config,
	}
}

func (bf *BloomFilter) Add(element []byte) {
	bf.bloomFilter.Add(element)
}

func (bf *BloomFilter) Check(element []byte) bool {
	return bf.bloomFilter.Check(element)
}

func (bf *BloomFilter) Exist(element []byte) bool {
	return bf.bloomFilter.Check(element)
}

func (bf *BloomFilter) DoesNotExist(element []byte) bool {
	return !bf.Exist(element)
}
