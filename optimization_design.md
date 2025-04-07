General Design of the Engine:

- Parser -> Creates an AST-Tree from the query using the query language
- Optimizer -> Applies selected optimization strategies to the AST-Tree
- Executor -> Executes the optimized AST-Tree and returns the results

Parser:
- We use the `lark` library to parse the query language into an AST-Tree

Optimizers:
- We have implemented the following optimization strategies:
    - Keyword merging
        - merges multiple keywords into a single keyword when they are on the same level on the AST-Tree
        - e.g. `kw('keyword1') AND kw('keyword2')` -> `kw('keyword1 AND keyword2')`
        - This results in a big performance improvement as we can now search for a single keyword instead of multiple keywords
    - Cost-based sorting
        - sorts the AST-Tree based on the estimated cost of the operations
        - With the order: Keyword -> Column name Predicate -> Percentile Predicate

Executors:
- Sequential Executor
    - Sequentially executes the AST-Tree using a bottom-up approach to evaluate the AST-Tree

- Prefilter Executor
    - The evaluation of Percentile Predicates using FAINDER can be sped up massively by using a filter
    - Cost-based sorting and Keyword merging should be applied before the creation of the read-and-write groups
    - To calculate the filter, we create read and write groups before the execution of the query
       - A list of read groups is the set of intermediary results, which intersection is the filter for a leaf node
       - The write group is the place where the result of the leaf node is stored

       - Rules for the creation of the read-and-write groups:
            - Using a top-down approach, we create the read and write groups for each node
            - If a node is a disjunction, give each child a new write group and only its write group as a read group
            - If a node is a conjunction, give each child a gets the same write group and read group as the parent node
            - If a node is a negation, we create a new write group and give it as the write and read group to the children nodes

    - The Prefilter Executor achieves significant performance improvements, reducing the execution time in realistic scenarios from up to 20 seconds to 0.5 seconds when a percentile predicate is used in the query in conjunction with a keyword predicate or Column name predicate.
        - This query form is the expected use case
    - The Overhead of the Prefilter Executor is negligible when the query does not contain a percentile predicate

- Threaded Executor
    - Cost-based sorting and Keyword merging should be applied before the use of the threaded executor
    - For the threaded executor, we use the `concurrent.futures` library to create a thread pool and execute the AST-Tree in different threads
    - This results in a significant performance improvement when percentile predicates are used in the query without a keyword predicate or column name predicate as a filter
    - The overhead is negligible when a query is just one predicate
    - The threaded executor is slower in cases where a filter could be used
    - Due to the GIL in Python, the threaded executor can not do everything in parallel

- Threaded Exceutor with Prefilter
    - Cost-based sorting and Keyword merging should be applied before the use of the threaded executor with prefilter
    - The threaded executor with prefilter is the best of both worlds
        - It combines the performance of the threaded executor with the prefilter executor
    - It combines threading every leaf node with the prefilter executor
        - To do that, it combines and resolves the results needed to construct the filter when a percentile predicate is evaluated
        - For all other nodes, it behaves like the threaded executor
    - The overhead is negligible when a query is just one predicate
