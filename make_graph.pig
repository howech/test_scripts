%default INPUT_FILE wikilinks/links-simple-sorted.txt
%default OUTPUT_FILE chimpmark-data/wikigraph.txt

A = load '$INPUT_FILE' using PigStorage(':') as (id:int,links:chararray);
B = FOREACH A GENERATE id as from_idx, FLATTEN(TOKENIZE(links)) as to_idx;
STORE B INTO '$OUTPUT_FILE' USING PigStorage('\t');
