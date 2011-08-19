rmf $output
inp = load '$input' as (type:chararray,count:int);

inp_ordered = ORDER inp BY count DESC;

STORE inp_ordered INTO '$output';