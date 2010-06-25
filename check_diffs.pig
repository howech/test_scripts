%default PIG_FILE    /user/ubuntu/title_count.txt/*
%default WUKONG_FILE /user/ubuntu/link_count/*

pig    = LOAD '$PIG_FILE'    using PigStorage() as (id:int,title:chararray,count:int);
wukong = LOAD '$WUKONG_FILE' using PigStorage() as (id:int,title:chararray,count:int);
cogrp  = COGROUP pig BY id, wukong BY id;
expand = FOREACH cogrp GENERATE group as id,FLATTEN( pig.title) as ptitle, FLATTEN( pig.count ) as pcount, FLATTEN( wukong.title ) as wtitle, FLATTEN( wukong.count ) as wcount;
filt   = FILTER expand BY (ptitle != wtitle) OR (pcount != wcount);
STORE filt into '/user/ubuntu/pigdiff';
