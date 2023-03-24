import csv
import os
import re

csv.field_size_limit(500 * 1024 * 1024)
keyword_list = ['node', 'network', 'leader', 'follower', 'election','delay',
                'cluster', 'consistent', 'replica', 'remote', 'recovery','clock']


def issue_filter(source):
    index = -1
    filter_index = 0
    Description_index = -1
    with open(source + '/output.csv', 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        header = ["Issue_key", "keyword"]
        writer.writerow(header)

        for _, _, files in os.walk(''r'' + source):
            for file in files:
                print(file)
                csv_file = open(source + '/' + file, 'r', encoding='utf-8', newline='')
                csvReader = csv.reader(csv_file)
                for row in csvReader:
                    index += 1
                    if index == 0:
                        Description_index = row.index('Description')
                        continue
                    try:
                        Summary = row[0]
                        Issue_key = row[1]
                        Description = row[Description_index]
                    except:
                        continue
                    print("index: {0}, Issue_key: {1}".format(index, Issue_key))
                    key_res = []
                    for key in keyword_list:
                        pattern = re.compile(key)
                        res = pattern.findall(Summary)
                        res += pattern.findall(Description)
                        key_res += res
                    count = len(key_res)
                    print("keyword : {0}, count: {1}".format(key_res, count))
                    if count > 0:
                        filter_index += 1
                        writer.writerow([Issue_key, key_res])
                print("index: {0}, filter_index: {1}".format(index, filter_index))
                csv_file.close()
    f.close()


if __name__ == '__main__':
    source_list = ['CAS', 'HADOOP', 'HBASE', 'HDFS', 'KAFKA', 'ZK']
    source = '../source/ZK'
    issue_filter(source)
