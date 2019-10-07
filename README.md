
# Assignment 1. Map Reduce. Simple Search engine.
## Team: Savchuk Vladislav, Kamil Saitov, Pavel Nikulin, Miklashevskaya Daria
### Description of what have been done
After looking at the task and given example in the document, we have come up with this architecture:
![](https://i.imgur.com/LRoBQEh.png)
1. Enumerator receives the text corpus and counts the (word, doc_id) pair (MapReduce job)
2. IDF then counts respective IDF for each word (MapReduce job)
3. Indexer transforms the data to Map Representation of the document (DOC_id, {word:TF/IDF}) and stores the result on the disk 

After that indexer part is done, it's result is stored on the disk and will be used by relevance analyzer.

4. Relevance analizator calculates the relevance score of each documents for the given query (MapReduce job)
5. Content Extractor look in the all texts and gives the user ranked documents with title and link, prints it to the console and also saves on the disk (MapReduce job)


### Design decisions
- We decided not to implement the Vocabulary because it is inefficient to reference the file during each mapping
- We decided to use the relevance score based on dot product because this is simpler and easier to understand ☺


### Description of the results
- Firstly, create the index using ```tokyo@namenode:~$ hadoop jar Indexer.jar /EnWikiSmall/```
- Running the job with the query as an argument ```tokyo@namenode:~$ hadoop jar Query.jar russia 5 /EnWikiSmall/```
- The results of the search: ![](https://i.imgur.com/QaNApIB.png)

### Usage
Usage of Indexer.jar
```
Indexer pathToFiles [OPTIONS [PARAMS]]
pathToFiles - directory with files which will be indexed

OPTIONS:
--no-cleanup - do not remove intermediate results
```

Usage of Query.jar
```
Query yourQuery maxDocuments pathToFiles [OPTIONS]
yourQuery - query on which the search engine will search
maxDocuments - maximum documents to show in rankings
pathToFiles - files on which index was created and on which we will search

OPTIONS:
--no-cleanup - do not remove intermediate results
```

### Contributors
* Vladislav Savchuk - Implementation of TF/IDF and Content Extractor in Mapreduce, Deployment to the cluster, Optimization of the code, Writing the report
* Kamil Saitov - Implementation of Word-Doc count in Mapreduce, refactoring 
* Pavel Nikulin - Implementation of IDF in Mapreduce, implementation of Vocabulator in Mapreduce
* Miklashevskaya Daria - Implemenation of Query Analyzer, Document Ranker in Mapreduce, Report writing
* Everyone - Collaboration, Problem and Bug fixing

## References
- Link to the repository (master branch): https://github.com/Sariellee/bd-assignment-1
- [Mapreduce Tutorial](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- [Hadoop Documentation](https://hadoop.apache.org/docs/r3.2.1/)



