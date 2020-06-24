import csv 
import sys
import itertools as ITER
import collections as COLL
import operator as OPR


"""

Csv Reader from csv module to populate only required columns,
and change required name columns to lower case, select year from date.
Used next(reader) to skip first line.


"""

def get_data(input_location):
    
    with open(input_location, newline='') as f:
        
        datastack=[]
        
        reader = csv.reader(f, delimiter=',')
        a=next(reader) 
        
        Product = a.index('Product')
        Date_received = a.index('Date received')
        Company = a.index('Company')
        
        for row in reader:
            datastack.append(tuple([row[Product].lower(),row[Date_received][:4],row[Company].lower()]))
            
            
    return datastack


"""

-itertools.groupby to group on the two keys for unique pairs (after sorting on keys).
-len(a) is total number of complaint for group.
-Counter from collections to run counts(total distinct companies,% of most_common).
-itemgetter from operator(c level) for efficient index pull.
-Yield to create a generator from the groupby iterator for each row. 

"""


def prepare_data(data):
 
    key_func = OPR.itemgetter(0,1) 
    
    data.sort(key = key_func)
    
    for k,v in ITER.groupby(data, key_func): 
        
        a=tuple(OPR.itemgetter(2)(x) for x in [*v])
        
        yield (k[0],k[1],len(a),len(COLL.Counter(a).keys()),round((COLL.Counter(a).most_common(1)[0][1]/len(a))*100))


        
"""

csv writer to write rows to output efficiently by sending prepare_data generator
to csv writerows

"""


def write_output(output_location,data):
    
    with open(output_location, 'w') as f:
        
        writer = csv.writer(f, delimiter=',')
       
        
        writer.writerows(prepare_data(data))
            
"""

main function to apply functions accordingly

"""

def main():
    
    location = sys.argv[1]
    
    dataframe = get_data(location)
    
    output_location= sys.argv[2]
    
    write_output(output_location,dataframe)
    
    
if __name__ == '__main__':
    main()
