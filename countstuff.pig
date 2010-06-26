A = LOAD 'xxx' using PigStorage() as (name,id1,id2);
B = FOREACH A GENERATE name, (id1 IS NULL ? 'SECOND' : (id2 IS NULL ? 'FIRST' : 'BOTH')) as type;
C = GROUP B BY type;
D = FOREACH C GENERATE group,COUNT(B);
DUMP D;
