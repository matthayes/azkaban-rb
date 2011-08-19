rmf $job_root/input_grouped.txt
inp = load '$job_root/input.txt' as (type:chararray,count:int);

inp_grouped = GROUP inp BY type;
inp_grouped = FOREACH inp_grouped GENERATE group as type, SUM(inp.count) as count;

store inp_grouped into '$job_root/input_grouped.txt';