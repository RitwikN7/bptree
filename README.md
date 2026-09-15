# B+ Tree Storage & Indexing Engine

A high-performance relational database storage manager and B+ Tree indexing implementation developed by the Database Group at the Computer Sciences Department, University of Wisconsin-Madison.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [System Architecture](#system-architecture)
3. [Storage Layer: BlobFile vs. PageFile](#storage-layer-blobfile-vs-pagefile)
4. [B+ Tree Architecture & Data Structure](#b-tree-architecture--data-structure)
   - [Node Physical Layout & Metapage](#node-physical-layout--metapage)
   - [Internal Nodes (NonLeafNodeInt)](#internal-nodes-nonleafnodeint)
   - [Leaf Nodes (LeafNodeInt)](#leaf-nodes-leafnodeint)
   - [B+ Tree Operations & Algorithms](#b-tree-operations--algorithms)
5. [Mathematical Formulas & Derivations](#mathematical-formulas--derivations)
   - [Page Sizing & Type Constants](#page-sizing--type-constants)
   - [Leaf Node Capacity Formula](#leaf-node-capacity-formula)
   - [Non-Leaf Node Capacity Formula](#non-leaf-node-capacity-formula)
   - [Tree Fanout, Height & Capacity Bounds](#tree-fanout-height--capacity-bounds)
   - [Buffer Hash Table Sizing](#buffer-hash-table-sizing)
6. [Buffer Manager & Clock Replacement Algorithm](#buffer-manager--clock-replacement-algorithm)
   - [Purpose & Core Data Structures](#purpose--core-data-structures)
   - [Clock Replacement Algorithm (Second-Chance FIFO)](#clock-replacement-algorithm-second-chance-fifo)
   - [Buffer Manager Lifecycle Operations](#buffer-manager-lifecycle-operations)
7. [Building and Running the Project](#building-and-running-the-project)
   - [Prerequisites](#prerequisites)
   - [Linux Build Commands](#linux-build-commands)
   - [Test Suite Breakdown](#test-suite-breakdown)
   - [API Documentation](#api-documentation)
8. [Authors & License](#authors--license)

---

## Project Overview

BadgerDB is a modular database storage engine that provides fundamental database system components, including disk-backed slotted page files, an in-memory buffer pool manager with clock page replacement, sequential relation scanning iterators, and an unclustered secondary B+ Tree index supporting exact-match lookups and filtered range scans.

The B+ Tree index implementation organizes key-to-record mappings (`<key, RecordId>`) into fixed-size 8 KB pages that are managed transparently through the buffer pool.

---

## System Architecture

The BadgerDB engine is organized into four decoupled architectural layers:

```
+------------------------------------------------------------------+
|                    Application & Test Layer                      |
|                         src/main.cpp                             |
+---------------------------------+--------------------------------+
                                  |
                                  v
+---------------------------------+--------------------------------+
|                   Query & Access Methods Layer                   |
|  - FileScan (Heap Table Scan)    - BTreeIndex (Range/Point Scan) |
|  - PageIterator / FileIterator   - startScan / scanNext / endScan|
+---------------------------------+--------------------------------+
                                  |
                                  v
+---------------------------------+--------------------------------+
|                         Index Layer                              |
|  - BTreeIndex (Index creation, search, insertion, leaf/root split)|
|  - IndexMetaInfo (Metapage 1)    - NonLeafNodeInt (Internal Node)|
|  - LeafNodeInt (Leaf Node)       - PageKeyPair / RIDKeyPair      |
+---------------------------------+--------------------------------+
                                  |
                                  v
+---------------------------------+--------------------------------+
|                    Buffer Management Layer                       |
|  - BufMgr (Buffer Pool Manager)  - BufDesc (Frame Descriptors)   |
|  - BufHashTbl (Hash Index)       - Clock Algorithm (Page Evict)  |
|  - Page bufPool[numBufs]         - BufStats (Disk I/O Profiling) |
+---------------------------------+--------------------------------+
                                  |
                                  v
+---------------------------------+--------------------------------+
|                      Storage / File Layer                        |
|  - BlobFile (Raw 8 KB Pages for B+ Tree Indices & Metadata)      |
|  - PageFile (Slotted Record Pages for Heap Table Relations)      |
|  - Page (8192-byte block)       - File (POSIX stream wrapper)    |
+---------------------------------+--------------------------------+
                                  |
                                  v
+------------------------------------------------------------------+
|                       OS / Filesystem                            |
|             (Disk files: relationName, indexName.offset)         |
+------------------------------------------------------------------+
```

---

## Storage Layer: BlobFile vs. PageFile

The engine uses 8192-byte (`Page::SIZE = 8192`) fixed-size pages, but divides file handling into two specialized representations:

```
                      +-------------------+
                      |    File (Base)    |
                      +---------+---------+
                                |
               +----------------+----------------+
               |                                 |
               v                                 v
      +-----------------+               +-----------------+
      |    PageFile     |               |    BlobFile     |
      +-----------------+               +-----------------+
      | Slotted format  |               | Raw byte block  |
      | PageHeader +    |               | No slot headers |
      | PageSlot array  |               | Direct struct   |
      | For Heap Tuples |               | cast for B+ Tree|
      +-----------------+               +-----------------+
```

1. **`PageFile` (Slotted Page Format)**:
   - Used for relation/table data files.
   - Implements a slotted page architecture where each page maintains a `PageHeader` tracking free space bounds, slot allocations, and a `PageSlot` directory pointing to variable-length record payloads.
   - Tuples are addressed by `RecordId` (`page_number` + `slot_number`).

2. **`BlobFile` (Raw Unslotted Page Format)**:
   - Used exclusively for B+ Tree index files (`relationName.attrByteOffset`).
   - Treats each 8 KB page as an unformatted raw byte array.
   - Avoids the slot directory overhead, enabling in-memory B+ Tree node structures (`IndexMetaInfo`, `NonLeafNodeInt`, `LeafNodeInt`) to be directly cast onto the raw buffer pool page pointers.

---

## B+ Tree Architecture & Data Structure

The B+ Tree is a self-balancing search tree tailored for block storage. All data records (`RecordId` pointers) reside strictly in the leaf nodes, while internal (non-leaf) nodes hold routing keys and child page pointers. Leaf nodes are linked horizontally into a singly linked list to permit fast sequential scans.

### Node Physical Layout & Metapage

Every B+ Tree index file is managed as a `BlobFile` with the following page structure:

```
Page 1: Metapage (IndexMetaInfo)
+-----------------------------------------------------------------------+
| relationName (20B) | attrByteOffset (4B) | attrType (4B) | rootPageNo |
+-----------------------------------------------------------------------+

Page 2+: Internal Nodes (NonLeafNodeInt) or Leaf Nodes (LeafNodeInt)
```

- **Metapage (`IndexMetaInfo`)**:
  Stored permanently on Page 1 of the index file. It records:
  - `relationName`: Name of the base relation table (up to 20 bytes).
  - `attrByteOffset`: Byte offset of the indexed attribute within relation records.
  - `attrType`: Attribute datatype (`INTEGER = 0`, `DOUBLE = 1`, `STRING = 2`).
  - `rootPageNo`: `PageId` of the current B+ Tree root page. When splits cause the tree height to increase, `rootPageNo` is updated here.

---

### Internal Nodes (NonLeafNodeInt)

Internal nodes route tree traversals from the root down to leaf nodes. For $K$ keys, an internal node maintains $K + 1$ child page pointers:

```
+-----------------------------------------------------------------------------------+
| level (4B)                                                                        |
+-----------------------------------------------------------------------------------+
| keyArray[0]         | keyArray[1]         | ... | keyArray[1022]                  |
+---------------------+---------------------+-----+---------------------------------+
| pageNoArray[0]      | pageNoArray[1]      | ... | pageNoArray[1022] | pageNoArray[1023]
+-----------------------------------------------------------------------------------+

Routing rule:
  child(pageNoArray[0])       holds keys <  keyArray[0]
  child(pageNoArray[i])       holds keys >= keyArray[i-1] and < keyArray[i]
  child(pageNoArray[last])    holds keys >= keyArray[last-1]
```

- `level`: Set to `1` if the node's children are leaf nodes; set to `0` if the children are other internal nodes.
- `keyArray[INTARRAYNONLEAFSIZE]`: Sorted array of routing keys (capacity = 1023 keys). Unused slots hold `-1`.
- `pageNoArray[INTARRAYNONLEAFSIZE + 1]`: Array of child page identifiers (capacity = 1024 pointers).

---

### Leaf Nodes (LeafNodeInt)

Leaf nodes store the actual index entries and form a horizontal linked list:

```
+-----------------------------------------------------------------------------+
| keyArray[0]        | keyArray[1]        | ... | keyArray[681]               |
+--------------------+--------------------+-----+-----------------------------+
| ridArray[0]        | ridArray[1]        | ... | ridArray[681]               |
| (pageNo, slotNo)   | (pageNo, slotNo)   |     | (pageNo, slotNo)            |
+--------------------+--------------------+-----+-----------------------------+
| rightSibPageNo (4B) ---------------------> Points to next right LeafNodeInt |
+-----------------------------------------------------------------------------+
```

- `keyArray[INTARRAYLEAFSIZE]`: Sorted keys (capacity = 682 keys). Unused slots hold `-1`.
- `ridArray[INTARRAYLEAFSIZE]`: Corresponding `RecordId` structs locating the record on disk in the base `PageFile`.
- `rightSibPageNo`: `PageId` of the right-sibling leaf page (`Page::INVALID_NUMBER` or `-1` if rightmost leaf). Enables $O(1)$ transitions during range queries without parent navigation.

---

### B+ Tree Operations & Algorithms

```
                         [ Root Node ]
                        /             \
                       /   navigate()  \
                      v                 v
            [ Internal Node ]      [ Internal Node ]
               /         \            /         \
              v           v          v           v
          +-------+   +-------+  +-------+   +-------+
          | Leaf1 |-->| Leaf2 |->| Leaf3 |-->| Leaf4 |--> [-1]
          +-------+   +-------+  +-------+   +-------+
                  ========================>
                     scanNext() via rightSibPageNo
```

1. **Initialization (`BTreeIndex` Constructor)**:
   - Constructs the index filename as `<relationName>.<attrByteOffset>`.
   - If the file exists, opens it, loads Page 1 (`IndexMetaInfo`), validates metadata attributes, and assigns `rootPageNum`.
   - If the file does not exist, allocates Page 1 (metadata), a root non-leaf page, and an initial empty leaf page. Traverses the base relation using `FileScan` and calls `insertEntry()` for every existing record.

2. **Search / Descent (`navigate`, `get_next_node`, `go_to_leaf`)**:
   - Recursively reads pages from `rootPageNum` down to leaves.
   - At each internal node, performs linear/binary search on `keyArray` to find the smallest index where `keyArray[i] >= searchKey` or `keyArray[i] == -1`.
   - Follows `pageNoArray[i]`. If `level == 1`, branches to `go_to_leaf()`; otherwise recurses through `navigate()`.
   - Unpins intermediate pages immediately to minimize buffer footprint.

3. **Insertion & Node Splitting**:
   - **Leaf Insert (`insert_in_leaf`)**: Places the new `(key, rid)` in ascending sorted order by shifting higher elements rightward.
   - **Leaf Split (`split_leaf`)**:
     - Triggered when the target leaf already contains `INTARRAYLEAFSIZE` keys.
     - Allocates a new leaf page via `bufMgr->allocPage()`.
     - Split midpoint: `half = ceil(INTARRAYLEAFSIZE / 2) = 341`.
     - Moves upper half of entries `[half ... INTARRAYLEAFSIZE - 1]` into the new leaf page.
     - Inserts the incoming key into the appropriate leaf (left or right).
     - Updates sibling pointers:
       ```
       new_leaf->rightSibPageNo = old_leaf->rightSibPageNo;
       old_leaf->rightSibPageNo = new_page_number;
       ```
     - Propagates a copy of the new leaf's lowest key (`new_leaf->keyArray[0]`) and new page ID up to the parent non-leaf node via `PageKeyPair<int>` (Copy-Up mechanism).
   - **Non-Leaf Split (`split_non_leaf`)**:
     - Triggered when an internal node reaches `INTARRAYNONLEAFSIZE` keys.
     - Allocates a new internal page via `bufMgr->allocPage()`.
     - Median key is extracted and pushed up to its parent (Push-Up mechanism, not retained in the node).
     - Left and right child page pointers are partitioned accordingly.
   - **Root Growth (`root_updation`)**:
     - When the root node splits, `root_updation()` allocates a new root page.
     - Sets `keyArray[0] = push_up_key`, `pageNoArray[0] = old_root_page_number`, and `pageNoArray[1] = new_split_page_number`.
     - Updates `rootPageNum` and overwrites Page 1 (`IndexMetaInfo::rootPageNo`).

4. **Range Scanning (`startScan`, `scanNext`, `endScan`)**:
   - **`startScan(lowVal, lowOp, highVal, highOp)`**:
     - Verifies operator validity (`lowOp` must be `GT` or `GTE`; `highOp` must be `LT` or `LTE`). Throws `BadOpcodesException` otherwise.
     - Verifies range validity (`lowVal <= highVal`). Throws `BadScanrangeException` otherwise.
     - Traverses down the B+ Tree to find the leaf page holding the lower bound.
     - Locates the first entry satisfying `lowOp`.
     - Pins the active leaf page in the buffer pool, saves scan state (`currentPageNum`, `currentPageData`, `nextEntry`), and sets `scanExecuting = true`.
   - **`scanNext(outRid)`**:
     - Validates `scanExecuting == true`. Throws `ScanNotInitializedException` otherwise.
     - Checks if the current key satisfies `highOp`. If satisfied, copies `ridArray[nextEntry]` to `outRid` and increments `nextEntry`.
     - If the leaf is exhausted (`nextEntry == INTARRAYLEAFSIZE` or key is `-1`):
       - Reads `rightSibPageNo`. If `rightSibPageNo == -1`, throws `IndexScanCompletedException`.
       - Unpins the exhausted page, reads the next sibling into `currentPageData`, sets `nextEntry = 0`, and continues scanning.
     - If the key fails the `highOp` boundary condition, throws `IndexScanCompletedException`.
   - **`endScan()`**:
     - Unpins `currentPageData` from the buffer pool.
     - Clears scan variables and sets `scanExecuting = false`.

---

## Mathematical Formulas & Derivations

### Page Sizing & Type Constants

The exact constants defined by BadgerDB's architecture (`page.h`, `types.h`, `btree.h`) are:

```
Constant / Type                 Size in Bytes     Definition
---------------------------------------------------------------------------------
Page::SIZE                      8192 bytes        Database page block size (8 KB)
sizeof(PageId)                  4 bytes           std::uint32_t (file page address)
sizeof(SlotId)                  2 bytes           std::uint16_t (page slot offset)
sizeof(int)                     4 bytes           Standard 32-bit signed integer
sizeof(RecordId)                8 bytes           PageId(4B) + SlotId(2B) + SlotId padding(2B)
```

---

### Leaf Node Capacity Formula

In `LeafNodeInt` (`btree.h`), each leaf node page must hold:
1. One right-sibling page pointer: `rightSibPageNo` (`sizeof(PageId) = 4 bytes`).
2. An array of keys: `keyArray[INTARRAYLEAFSIZE]` (`sizeof(int) = 4 bytes` each).
3. An array of record pointers: `ridArray[INTARRAYLEAFSIZE]` (`sizeof(RecordId) = 8 bytes` each).

The capacity `INTARRAYLEAFSIZE` is derived by subtracting the sibling pointer overhead from the total page size and dividing by the combined size of one key-RID pair:

```
                       Page::SIZE - sizeof(PageId)
INTARRAYLEAFSIZE  =  -------------------------------
                      sizeof(int) + sizeof(RecordId)

                      8192 - 4
                  =  ----------
                       4 + 8

                      8188
                  =  ------
                       12

                  =  682.333...  ==>  682 entries (integer truncation)
```

**Leaf Page Space Utilization**:
```
Space allocated for keys:          682 * 4 bytes  =  2728 bytes
Space allocated for RecordIds:     682 * 8 bytes  =  5456 bytes
Space allocated for sibling ptr:   1 * 4 bytes    =     4 bytes
--------------------------------------------------------------
Total bytes occupied:                             =  8188 bytes
Unused / padding bytes:            8192 - 8188    =     4 bytes
```

**Leaf Split Threshold**:
```
Leaf split midpoint = ceil(INTARRAYLEAFSIZE / 2)
                    = ceil(682 / 2)
                    = 341 entries
```

---

### Non-Leaf Node Capacity Formula

In `NonLeafNodeInt` (`btree.h`), each internal node page must hold:
1. One level indicator: `level` (`sizeof(int) = 4 bytes`).
2. An array of routing keys: `keyArray[INTARRAYNONLEAFSIZE]` (`sizeof(int) = 4 bytes` each).
3. An array of child page pointers: `pageNoArray[INTARRAYNONLEAFSIZE + 1]` (`sizeof(PageId) = 4 bytes` each).

Notice that $K$ keys require $K + 1$ page pointers. By reserving space for the first base child pointer `pageNoArray[0]` alongside `level`, each additional key pairs with one additional child pointer:

```
                          Page::SIZE - sizeof(int) - sizeof(PageId)
INTARRAYNONLEAFSIZE  =  -------------------------------------------
                                sizeof(int) + sizeof(PageId)

                          8192 - 4 - 4
                     =  ----------------
                             4 + 4

                          8184
                     =  -------
                           8

                     =  1023 keys
```

**Non-Leaf Page Space Utilization**:
```
Space allocated for level:                    1 * 4 bytes    =     4 bytes
Space allocated for keys:                  1023 * 4 bytes    =  4092 bytes
Space allocated for child page pointers:   1024 * 4 bytes    =  4096 bytes
--------------------------------------------------------------------------
Total bytes occupied:                      4 + 4092 + 4096   =  8192 bytes
Unused / padding bytes:                    8192 - 8192       =     0 bytes (100% exact fit)
```

**Internal Node Fan-out**:
```
Internal Node Fan-out (Child Pointers) = INTARRAYNONLEAFSIZE + 1 = 1024
```

---

### Tree Fanout, Height & Capacity Bounds

Let:
- $B = 1024$ be the internal node fan-out (child pointers per non-leaf node).
- $M_{leaf} = 682$ be the maximum leaf capacity (records per leaf).
- Under classic B+ Tree invariants, every non-root node is at least half full:
  - Minimum internal node fan-out: $\lceil B / 2 \rceil = 512$ pointers.
  - Minimum leaf occupancy: $\lceil M_{leaf} / 2 \rceil = 341$ records.

For a tree of height $h$ indexing $N$ records:

```
                    /        N        \
h  <=  ceil( log   | ----------------- | ) + 1
                512 \       341       /
```

**Theoretical Maximum Record Capacity by Height**:

```
Height (h)   Node Structure                       Maximum Indexed Records
-----------------------------------------------------------------------------------------
h = 1        Root is a single leaf node           682 records
h = 2        1 Root + up to 1,024 Leaves          1,024 * 682 = 698,368 (~700K records)
h = 3        1 Root + 1,024 Non-Leaves            1,024 * 1,024 * 682
             + 1,048,576 Leaves                   = 715,128,832 (~715 million records)
```

A 3-level B+ Tree in BadgerDB can index over 715 million tuples, requiring at most 3 page accesses from root to leaf for any point lookup.

**I/O Cost Equations**:
- **Point Search I/O**:
  $$\text{Cost}_{point} = O(h) \le 3 \text{ page reads}$$
- **Range Query I/O (retrieving $R$ matching records)**:
  $$\text{Cost}_{range} = O\left(h + \left\lceil \frac{R}{M_{leaf}} \right\rceil\right) = O\left(h + \left\lceil \frac{R}{682} \right\rceil\right) \text{ page reads}$$

---

### Buffer Hash Table Sizing

To ensure $O(1)$ page lookups in the buffer pool, `BufHashTbl` dimensions its bucket count based on `numBufs` (`buffer.cpp`):

```
htsize = ( ( (int)(numBufs * 1.2) * 2 ) / 2 ) + 1
       = floor(numBufs * 1.2) + 1
```

For the default test buffer pool size `numBufs = 100`:
```
htsize = floor(100 * 1.2) + 1 = 120 + 1 = 121 buckets
```

**Hash Mapping Function** (`bufHashTbl.cpp`):
```
hash(file, pageNo) = ( (long)file + pageNo ) % HTSIZE
```
Combines the memory pointer of the `File` object with the target `PageId` modulo the prime-like bucket count to evenly disperse pages across buckets and prevent collisions.

---

## Buffer Manager & Clock Replacement Algorithm

### Purpose & Core Data Structures

The Buffer Manager (`BufMgr`) mediates all read and write requests between database components (`BTreeIndex`, `FileScan`) and physical disk files. Disk I/O is multiple orders of magnitude slower than RAM access; the buffer pool minimizes disk transfers by caching frequently queried pages in memory.

```
+-----------------------------------------------------------------------------+
|                                  BufMgr                                     |
|                                                                             |
|  +-----------------------------------------------------------------------+  |
|  | bufPool: Page[0] | Page[1] | Page[2] | ... | Page[numBufs - 1]        |  |
|  +-----------------------------------------------------------------------+  |
|                                                                             |
|  +-----------------------------------------------------------------------+  |
|  | bufDescTable: BufDesc[0] | BufDesc[1] | ... | BufDesc[numBufs - 1]     |  |
|  | [frameNo, file*, pageNo, pinCnt, dirty, valid, refbit]                |  |
|  +-----------------------------------------------------------------------+  |
|                                                                             |
|  +-----------------------------+          +------------------------------+  |
|  | BufHashTbl: (file,page)->fr |          | clockHand: Circular index    |  |
|  +-----------------------------+          +------------------------------+  |
+-----------------------------------------------------------------------------+
```

1. **`bufPool`**: Array of `Page` objects allocated in memory of size `numBufs` (each 8192 bytes).
2. **`bufDescTable`**: Parallel array of `BufDesc` descriptor objects tracking the status of each frame:
   - `file`: Pointer to the `File` that owns the page.
   - `pageNo`: ID of the page loaded in this frame.
   - `pinCnt`: Number of active concurrent callers accessing this page. A page cannot be evicted if `pinCnt > 0`.
   - `dirty`: `true` if the page has been modified in memory. Must be written back to disk before frame reuse.
   - `valid`: `true` if the frame contains a valid disk page; `false` if uninitialized.
   - `refbit`: Usage bit inspected by the Clock algorithm (set to `true` whenever accessed or pinned).
3. **`hashTable` (`BufHashTbl`)**: Hash table resolving `(File*, PageId)` pairs to `FrameId` in average $O(1)$ time with separate chaining for collision resolution.
4. **`bufStats`**: Tracks performance telemetry:
   - `accesses`: Count of page lookups.
   - `diskreads`: Count of physical disk page loads.
   - `diskwrites`: Count of physical disk page writes.

---

### Clock Replacement Algorithm (Second-Chance FIFO)

When a page request misses in the buffer pool and no unallocated frames exist, the buffer manager must evict an existing page. BadgerDB uses the **Clock Algorithm**, an $O(1)$ approximation of Least Recently Used (LRU) that eliminates the heavy overhead of timestamps or doubly linked list maintenance on every hit.

```
                      Frame 0 [valid=1, pinCnt=0, refbit=1]
                                 /            \
                                /              \
  Frame 99 [valid=1, pin=0, ref=0]            Frame 1 [valid=1, pin=1, ref=1]
               |                                     |
               |             (clockHand)             |
               |                  |                  |
               |                  v                  |
  Frame 98 [valid=1, pin=0, ref=1]            Frame 2 [valid=0] <--- Immediately reusable!
                                \              /
                                 \            /
                      Frame 3 [valid=1, pin=0, ref=0] <--- EVICTION CANDIDATE
```

#### Step-by-Step Clock Replacement Logic (`allocBuf`):

```
                       advanceClock(): clockHand = (clockHand + 1) % numBufs
                                                 |
                                                 v
                                       Is frame[clockHand] valid?
                                      /                          \
                               (No)  /                            \  (Yes)
                                    v                              v
                           [ Use this frame ]             Is refbit == true?
                                                         /                  \
                                                  (Yes) /                    \ (No)
                                                       v                      v
                                             Clear refbit = false;     Is pinCnt == 0?
                                             Increment accesses;      /               \
                                             Advance clock.    (Yes) /                 \ (No)
                                                                    v                   v
                                                           [ EVICT FRAME ]      [ Page in use,
                                                           1. Remove from hash   skip & advance ]
                                                           2. If dirty: flush
                                                           3. Clear BufDesc
                                                           4. Return frame
```

1. **Advance Clock**: `clockHand = (clockHand + 1) % numBufs`.
2. **Examine Frame**:
   - **Case 1 (Invalid Frame)**: If `valid == false`, the frame is unused. It is immediately selected for allocation.
   - **Case 2 (Referenced Frame)**: If `refbit == true`, the page was accessed recently. The clock gives it a "second chance": it clears `refbit = false`, increments `bufStats.accesses`, and advances `clockHand`.
   - **Case 3 (Candidate for Eviction)**: If `refbit == false` and `pinCnt == 0`:
     - Frame is selected for replacement.
     - The old `(file, pageNo)` entry is removed from `hashTable`.
     - If `dirty == true`, the page is written back to disk via `file->writePage()`, and `bufStats.diskwrites` is incremented.
     - The descriptor is reset via `Clear()`.
     - `frame = clockHand` is returned.
3. **Loop Bound**: The algorithm sweeps through the buffer pool up to 2 full revolutions ($2 \times \text{numBufs}$ iterations).
4. **Buffer Full Exception**: If all frames remain pinned (`pinCnt > 0`) after 2 full sweeps, the manager throws `BufferExceededException`.

---

### Buffer Manager Lifecycle Operations

- **`readPage(file, pageNo, page)`**:
  Queries `hashTable`.
  - *Cache Hit*: Sets `refbit = true`, increments `pinCnt++`, assigns `page = &bufPool[frameNo]`.
  - *Cache Miss*: Calls `allocBuf(frameNo)` to obtain a frame via the Clock algorithm, reads page from disk via `file->readPage()` (`diskreads++`), initializes `BufDesc::Set(file, pageNo)`, inserts the mapping into `hashTable`, and assigns `page`.
- **`unPinPage(file, pageNo, dirty)`**:
  Decrements `pinCnt`. If `dirty == true`, marks `BufDesc::dirty = true`. If called on a page with `pinCnt == 0`, throws `PageNotPinnedException`.
- **`allocPage(file, pageNo, page)`**:
  Invokes `allocBuf(frameNo)`, allocates a new page on disk via `file->allocatePage(pageNo)`, binds the frame to `pageNo`, and records it in `hashTable`.
- **`flushFile(file)`**:
  Scans all frames belonging to `file`. If any frame has `pinCnt > 0`, throws `PagePinnedException`. Writes all dirty pages to disk, removes them from `hashTable`, and resets their descriptors.
- **`disposePage(file, pageNo)`**:
  Deletes the page from disk via `file->deletePage(pageNo)` and purges its cached frame from `hashTable` and `bufDescTable`.

---

## Building and Running the Project

### Prerequisites

Standard Linux development packages are required:
- **C++ Compiler**: GCC (g++ version 4.6 or higher) or Clang with C++0x/C++11 support.
- **Build Utility**: GNU Make (`make`).
- **Archiver**: `ar` (standard in `binutils`).
- **Documentation Generator** (optional): Doxygen (`doxygen`, version 1.4+).

On Ubuntu/Debian-based distributions, install the toolchain with:
```bash
sudo apt-get update
sudo apt-get install build-essential doxygen
```

---

### Linux Build Commands

All build commands must be executed from the repository root directory:

```bash
# 1. Clean previous object files, libraries, and binaries
make clean

# 2. Compile all static libraries, modules, and link badgerdb_main
make

# 3. Execute the test suite
./src/badgerdb_main

# 4. (Optional) Generate Doxygen documentation
make doc
```

#### Build Artifacts Produced:
- `src/lib/bufmgr.a`: Static archive containing buffer manager, file system, page storage, and hash table objects.
- `src/lib/exceptions.a`: Static archive of all BadgerDB exception classes.
- `src/obj/`: Compiled object files (`*.o`).
- `src/badgerdb_main`: Final test binary.

---

### Test Suite Breakdown

Executing `./src/badgerdb_main` runs the comprehensive verification battery implemented in `src/main.cpp`:

1. **Relation Creation Tests**:
   - `createRelationForward()`: Creates a table relation with 5,000 tuples in ascending key order ($i = 0, 1, \dots, 4999$). Builds a B+ Tree index on the integer field.
   - `createRelationBackward()`: Inserts 5,000 tuples in reverse key order ($i = 4999, 4998, \dots, 0$). Verifies reverse-order leaf splitting and balanced root elevation.
   - `createRelationRandom()`: Generates a randomized permutation of 5,000 tuples. Tests arbitrary leaf splits, internal splits, and balance maintenance.
2. **Range Scan Validations (`intScan`)**:
   - Executes range queries across boundary conditions:
     - `(25, 40)`: Open interval (`GT`, `LT`) -> Expects 14 records.
     - `[20, 35]`: Closed interval (`GTE`, `LTE`) -> Expects 16 records.
     - `(-3, 3)`: Range crossing zero (`GT`, `LT`) -> Expects 3 records.
     - `(996, 1001)`: Upper region (`GT`, `LT`) -> Expects 4 records.
     - `(0, 1)`: Empty match interval (`GT`, `LT`) -> Expects 0 records.
     - `(300, 400)`: Wide interval (`GT`, `LT`) -> Expects 99 records.
     - `[3000, 4000)`: Large multi-page interval (`GTE`, `LT`) -> Expects 1,000 records.
3. **Exception and Error Handling Tests (`errorTests`)**:
   - `endScan()` before `startScan()` -> Validates `ScanNotInitializedException`.
   - `scanNext()` before `startScan()` -> Validates `ScanNotInitializedException`.
   - Invalid scan opcodes (e.g., `LTE` for low operator) -> Validates `BadOpcodesException`.
   - Inverted scan boundaries (`lowVal > highVal`) -> Validates `BadScanrangeException`.
   - Unmatched queries -> Validates `NoSuchKeyFoundException`.

---

### API Documentation

To generate HTML API documentation from Doxygen comments:

```bash
make doc
```

To view the generated documentation, open the generated index in your web browser:
```bash
xdg-open docs/index.html
# or
firefox docs/index.html
```
