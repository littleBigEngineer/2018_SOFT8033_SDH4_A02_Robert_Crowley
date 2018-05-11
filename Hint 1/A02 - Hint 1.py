import json

def clean(line):
  return line.values()[0], (line.values()[3], line.values()[6])

def reduce(rdd):
  sort = []
  cus_dict = {}
  for line in rdd.collect():  
    neg_val = 0
    score = 0
    if not line[0] in cus_dict:
      if line[1][1] == "Negative":
        neg_val = 1
        score = score + (line[1][0] * -1)
      else:
        score = score + line[1][0]
      cus_dict[line[0]] = (1, neg_val, score)
    else:
      if line[1][1] == "Negative":
        neg_val = 1;
        score += (line[1][0] * -1)
      else:
        score += line[1][0]
      list = cus_dict[line[0]]
      cus_dict[line[0]] = (list[0]+1, list[1]+neg_val, list[2]+score)
      
  for d in cus_dict.items():
    sort.append(d)
  
  return sc.parallelize(sort)

def filter(item, average, percentage):
  keep = True
  if item[1][0] <= average:
    keep = False
    
  curr_percentage = (float(item[1][1])/float(item[1][0]))*100.0
  if curr_percentage > percentage:
    keep = False;
  return keep

def order(item):
  average_score = float(item[1][2])/float(item[1][0])
  return item[0], (item[1][0], item[1][1], item[1][2], average_score)

def my_main(dataset_dir, result_dir, percentage_f):
 
  inputRDD = sc.textFile(dataset_dir)
  mapRDD = inputRDD.map(lambda x: json.loads(x))
  cleanRDD = mapRDD.map(lambda x: clean(x))
  reducedRDD = reduce(cleanRDD)
  
  average_review = inputRDD.count()/reducedRDD.count()
  
  filteredRDD = sortedRDD.filter(lambda x:  filter(x, average_review, percentage_f))
  sortMapRDD = filteredRDD.map(lambda x: order(x))
  outputRDD = sortMapRDD.sortBy(lambda x: -x[1][3])
  
  for i in outputRDD.collect():
    print(i)
    
  outputRDD.saveAsTextFile(result_dir)
  
if __name__ == '__main__':
    source_dir = "/FileStore/tables/A02/my_dataset/"
    result_dir = "/FileStore/tables/A02/my_result/"
    percentage_f = 10
    dbutils.fs.rm(result_dir, True)
    my_main(source_dir, result_dir, percentage_f)
