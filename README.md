  # KVStorage: High-Performance Key-Value Store

**KVStorage** is a high-performance key-value store inspired by RocksDB. It uses a **Log-Structured Merge (LSM) tree** to optimize data storage and retrieval, making it ideal for write-heavy workloads on SSDs.

---

## Key Components:
- **MemTable:** In-memory storage for fast writes, backed by a **Write-Ahead Log (WAL)** for durability.
- **SST Files:** Immutable, sorted files stored on disk. Compactions merge smaller files into larger ones for efficient queries.
- **Block Cache:** Caches frequently accessed data for faster reads.
- **Bloom Filters:** Quickly check for key existence, minimizing disk access.

---

## How It Works:
1. **Writes:** Data is stored in the MemTable and logged in the WAL. When the MemTable fills up, it is flushed to an SST file.
2. **Reads:** Queries check the MemTable, Block Cache, and SST files, aided by Bloom filters for faster lookups.
3. **Compactions:** Periodically merges SST files to reduce fragmentation and optimize performance.

---

## Features:
- High write throughput and read efficiency.
- Durable and crash-safe with WAL-based recovery.
- Customizable compaction and caching strategies.

---

## Getting Started:
```bash
git clone https://github.com/yourusername/kvstorage.git
cd kvstorage
go run
