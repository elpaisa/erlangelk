# erlangelk

Run:
    bash dev.sh
    
    Eshell V8.2  (abort with ^G)
    (erl@local)1> elasticsearch:start().
    (erl@local)1> elasticsearch:create_index("index-name").
    (erl@local)1> elasticsearch:index_exists("index-name").

**WARNING:**  This will create and delete a test-index
    Test
    
    Eshell V8.2  (abort with ^G)
    (erl@local)1> elasticsearch:test().
    

Function | Parameters | Description
----- | ----------- | --------
create_index/1 | "index-name" | Checks if the index exists, if not, creates it
create_index/2 | "index-name", {Shards, Replicas}  | Creates an index with settings
path_exists/1 | "path" | checks a specific path in Elasticsearch "index-name/mapping-name/docId"
bulk_index/2 | {Index, Type}, IndexTypeIdJsonTup | uses bulk api to push documents into elasticsearch, see [Bulk Api](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/docs-bulk.html)



Most of this code was taken from 

* [erlasticsearch-erlang](https://github.com/snorecone/elasticsearch-erlang) - didn't work well for https, and some needed methods missing.


An erlang client for elasticsearch's REST interface

## other elasticsearch clients and why this was made

* [erlasticsearch](https://github.com/dieswaytoofast/erlasticsearch) - too much frustration with thrift.
* [erlastic_search](https://github.com/tsloughter/erlastic_search) - dependency on hackney for http requests (built-in pooling is broken); unable to configure without wrapping the module.

