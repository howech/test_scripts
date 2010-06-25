%default GRAPH /chimpmark-data/wikigraph.txt
%default TITLES /chimpmark-data/titles-indexed.txt
%default OUTPUT /user/ubuntu/title_count.txt

graph = LOAD '$GRAPH' using PigStorage() as (from_id:int,to_id:int);
graph_group = GROUP graph by to_id;
in_count = FOREACH graph_group GENERATE group as id, COUNT(graph) as count; 
titles = LOAD '$TITLES' using PigStorage() as (id:int,title:chararray);
title_count_0 = JOIN in_count by id, titles by id ;
title_count = FOREACH title_count_0 GENERATE $0 as id, $3 as title, $1 as count;
EXPLAIN title_count;
--STORE title_count into '$OUTPUT' using PigStorage();
