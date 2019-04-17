CREATE EXTENSION toytable;

create table toytab (i int4, j int4, k int4) using toytable;

select * from toytab;

delete from toytab;

insert into toytab values(1, 2, 3);

update toytab set i = 4;

copy toytab from stdin;
