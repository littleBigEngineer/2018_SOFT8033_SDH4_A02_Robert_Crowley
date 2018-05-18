import time
from pyspark.streaming import StreamingContext
import json

total = 0
cuisine = 0
average = 0.0

def group(rdd):
  global total, cuisine, average
  total = 0
  cuisine = 0
  average = 0.0
  
  total = rdd.count()
  counted = rdd.groupBy(lambda line: line[0])
  cuisine = counted.count()
  
  if total != 0 and cuisine != 0:
    average = float(total/cuisine)
  print(average)
  return counted
  
def reduce(rdd):
  neg_val = 0 
  score = 0 
  num_reviews = 0
  for line in rdd[1]:
    num_reviews += 1
    if line[1][1] == "Negative":
      neg_val += 1
      score += (line[1][0] * -1)
    else:
      score += line[1][0]
  return (rdd[0], (num_reviews, neg_val, score, float(score)/float(num_reviews)))
  
def filterEntry(line, percentage):
  result = (float(line[1][1])/float(line[1][0]))*100.0
  if result > percentage or line[1][0] <= average:
    return False
  else:
    return True
  
def reduceFields(line):
  return (line["cuisine"], (line["points"], line["evaluation"]))

def my_model(ssc, monitoring_dir, result_dir, percentage_f):
    inputDStream = ssc.textFileStream(monitoring_dir)
    jsonMapDStream = inputDStream.map(lambda x: json.loads(x))
    reducedFieldsDStream = jsonMapDStream.map(lambda x: reduceFields(x))
    transformDStream = reducedFieldsDStream.transform(lambda rdd: group(rdd))
    cuisineDStream = transformDStream.map(lambda rdd: reduce(rdd))
    filteredDStream = cuisineDStream.filter(lambda x: filterEntry(x, percentage_f))
    sortedDStream = filteredDStream.transform(lambda x: x.sortBy(lambda y: y[1][3], ascending=False))
    sortedDStream.pprint()

def create_ssc(monitoring_dir, result_dir, max_micro_batches, time_step_interval, percentage_f):
    ssc = StreamingContext(sc, time_step_interval)
    ssc.remember(max_micro_batches * time_step_interval)
    my_model(ssc, monitoring_dir, result_dir, percentage_f)
    return ssc

def get_source_dir_file_names(source_dir, verbose):
    res = []
    fileInfo_objects = dbutils.fs.ls(source_dir)
    for item in fileInfo_objects:
        file_name = str(item)
        if verbose == True:
            print(file_name)
        lb_index = file_name.index("name=u'")
        file_name = file_name[(lb_index + 7):]
        ub_index = file_name.index("',")
        file_name = file_name[:ub_index]
        res.append(file_name)
        if verbose == True:
            print(file_name)
    return res

def streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose):
    files = get_source_dir_file_names(source_dir, verbose)
    start = time.time()
    count = 0
    for file in files:
        count += 1
        dbutils.fs.cp(source_dir + file, monitoring_dir + file)
        time.sleep(start+(count*time_step_interval)-time.time())

def my_main(source_dir, monitoring_dir, checkpoint_dir, result_dir, max_micro_batches, time_step_interval, verbose, percentage_f):
    ssc = StreamingContext.getActiveOrCreate(checkpoint_dir, lambda: create_ssc(monitoring_dir, result_dir, max_micro_batches, time_step_interval, percentage_f))
    ssc.start()
    ssc.awaitTerminationOrTimeout(time_step_interval)
    streaming_simulation(source_dir, monitoring_dir, time_step_interval, verbose)
    ssc.stop(stopSparkContext=False)
    if (not sc._jvm.StreamingContext.getActive().isEmpty()):
        sc._jvm.StreamingContext.getActive().get().stop(False)

if __name__ == '__main__':
    source_dir = "/FileStore/tables/A02/my_dataset/"
    monitoring_dir = "/FileStore/tables/A02/my_monitoring/"
    checkpoint_dir = "/FileStore/tables/A02/my_checkpoint/"
    result_dir = "/FileStore/tables/A02/my_result/"
    dataset_micro_batches = 16
    time_step_interval = 3
    max_micro_batches = dataset_micro_batches + 1
    verbose = False
    percentage_f = 10
    dbutils.fs.rm(monitoring_dir, True)
    dbutils.fs.rm(result_dir, True)
    dbutils.fs.rm(checkpoint_dir, True)
    dbutils.fs.mkdirs(monitoring_dir)
    dbutils.fs.mkdirs(result_dir)
    dbutils.fs.mkdirs(checkpoint_dir)
    my_main(source_dir,
            monitoring_dir,
            checkpoint_dir,
            result_dir,
            max_micro_batches,
            time_step_interval,
            verbose,
            percentage_f
            )
