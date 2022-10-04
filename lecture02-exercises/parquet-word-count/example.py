from pydoc import cli
from hdfs import InsecureClient
from collections import Counter
import pyarrow.parquet as pq
import pandas as pd
import pyarrow as pa

client = InsecureClient('http://namenode:9870', user='root')

# Make wordcount reachable outside of the with-statement
wordcount = None

with client.read('/alice-in-wonderland.txt', encoding='utf-8') as reader:
    wordcount = Counter(reader.read().split()).most_common(10)


print(wordcount)
df = pd.DataFrame(wordcount)
table = pa.Table.from_pandas(df)
pq.write_table(table, "/parquetwordssecond.parquet")

#client.upload("/", "/parquetwordssecond.parquet")

client.download("/parquetwordssecond.parquet", "/downloadedparquet.parquet")

resultTable = pq.read_table("/downloadedparquet.parquet")
test = resultTable.to_pandas()
print(test)


# To-Do: Save the wordcount in a Parquet file and read it again!