# CS201-G3T1

## Project Overview
Our project explores and refines various in-memory data structures to analyze their strengths and weaknesses for query handling and preprocessing in SQL-like systems. The goal is to implement and evaluate multiple data structures to optimize performance for both bulk data processing and query execution.

We focused on:
- Analyzing different in-memory data structures
- Comparing their benefits and trade-offs
- Implementing these structures for query and data management

## Key Structures Explored
1. Indices
2. Binary Search Tree
3. B+ Tree
4. Log Structured Merge Tree
5. Cache
6. Bloom Filters

## Directory Structure
```plaintext
CS201-sql-project/
├── src/
│   └── main/java/edu/smu/smusql/
│       ├── bloomfilter/
│       ├── bplus/
│       ├── bst/
│       ├── cache/
│       ├── evaluator/
│       ├── interfaces/
│       ├── lsm/
│       ├── parser/
│       ├── table/
│       ├── Engine.java
│       └── Main.java
│   └── test/java/edu/smu/smusql/
│       └── EngineTest.java
├── target/
├── .gitignore
└── pom.xml
```

## Setup
1. Clone the repository
   ```bash
   git clone https://github.com/ruicongg/CS201-sql-project.git
   ```
2. Navigate to the project directory:
    ```bash
   cd CS201-sql-project
   ```
3. Compile the project with Maven:
   ```bash
   mvn compile
   ```
4. Run the application:
   ```bash
   mvn exec:java
   ```

## Changing the underlying data structure
In line 23 of `Engine.java`, change `storageInterface` accordingly.
```java
private final StorageInterface storageInterface = new LSMStorage();
```
The following `StorageInterface` are available for testing
- BPlusTreeStorage
- BSTStorage
- IndicesStorage
- LSMStorage

Additionally, the bloom filter can be disabled by removing the arguments in its constructor.
```java
// Bloom Filter disabled
private final BloomFilter bloomFilter = new BloomFilter();

// Bloom Filter enabled
private final BloomFilter bloomFilter = new BloomFilter(FILTER_SIZE, HASH_COUNT);
```

For cache, you may set `CACHE_CAPACITY` on line 45 to disable it.
```java
// Cache disabled
private static final int CACHE_CAPACITY = 0;
private final ResultCache resultCache = new ResultCache(CACHE_CAPACITY);

// Cache enabled with capacity of 10000
private static final int CACHE_CAPACITY = 10000;
private final ResultCache resultCache = new ResultCache(CACHE_CAPACITY);
```

## Evaluation
A sample of the evaluation is provided below
```
1. Enter 'exit' to exit
2. Enter 'evaluate' to evaluate the engine
3. Enter 'lsm' to test the lsm implementation
4. Enter 'bst' to test the BST implementation 
```
smusql> *evaluate*
```
Please configure the initial database environment.
How many rows to prepopulate into USERS table? 
```
smusql> *10000*
```
How many rows to prepopulate into PRODUCTS table? 
```
smusql> *10000*
```
How many rows to prepopulate into ORDERS table? 
```
smusql> *10000*
```
Setting up the evaluation environment...
Prepopulating users...
Prepopulating orders...
Prepopulating products...
How would you like to evaluate the engine?
1. Enter 'random' for random percentages of queries
2. Enter 'interactive' to select each percentages of queries 
```
smusql> *interactive*

Enter percentage for INSERT queries: *0*

Enter percentage for DELETE queries: *0*

Enter percentage for UPDATE queries: *0*

Enter percentage for SELECT queries: *100*
```
INSERT:0.0%
DELETE:0.0%
UPDATE:0.0%
SELECT:100.0%
Evaluator configured as Evaluator{insertPercentage=0.0, deletePercentage=0.0, updatePercentage=0.0, selectPercentage=1.0}
How many queries would you like to execute? 
```
smusql> *1000*
```
What % of queries do you want to be complex? 
```
smusql> *0*
```
Executing 0 complex select queries...
Executing 0 complex update queries...
Executing 0 simple insert queries...
Executing 0 simple delete queries...
Executing 0 simple update queries...
Executing 1000 simple select queries...
Ran 0 simple select queries...
Simple Select Speed: 0.8104785000000001 milliseconds per query.
Simple Insert Speed: Infinity milliseconds per query.
Simple Update Speed: Infinity milliseconds per query.
Simple Delete Speed: Infinity milliseconds per query.
Complex Select Speed: Infinity milliseconds per query.
Complex Update Speed: Infinity milliseconds per query.
Memory usage: 127 MB.
Time elapsed: 0.816465166 seconds
```

## Contributors
Built by: 

- Champion CHALLANDER
- Zane CHEE Jun Yi
- KWAN Rui Cong
- Vince TAN Yueh Yang
- TEO Fu Qiang
- WONG Yu Hung