rmf $output
inp = load '$input' as (type:chararray,count:int);

inp_grouped = GROUP inp BY type;
inp_grouped = FOREACH inp_grouped GENERATE group as type, SUM(inp.count) as count;

store inp_grouped into '$output';