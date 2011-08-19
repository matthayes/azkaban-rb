rmf $job_root/input_ordered.txt
inp = load '$job_root/input_grouped.txt' as (type:chararray,count:int);

inp_ordered = ORDER inp BY count DESC;

STORE inp_ordered INTO '$job_root/input_ordered.txt';