import csv
import os
import re
import pandas as pd

csv.field_size_limit(500 * 1024 * 1024)


def key_issue_selector():
    data = pd.read_csv('select.csv')
    # with open('./final.csv', 'a', encoding='utf-8', newline='') as f:
    #     writer = csv.writer(f)
    #     header = ["Issue_key"]
    #     writer.writerow(header)
    #     csvReader = csv.reader(csv_file)
    #     index=0
    #     for row in csvReader:
    #         index += 1
    #         try:
    #             Issue_key = row[1]
    #         except:
    #             continue
    #         writer.writerow([Issue_key])
    #         print("index: {0}".format(index))
    #         csv_file.close()

    sample = data['Issue_key'].sample(n=100, axis=0)
    sample.to_csv('final1.csv', index=False, encoding='utf_8')


def issue(source_list):
    for issue_str in source_list:
        source = '../source/' + issue_str
        index = -1
        filter_index = 0
        Description_index = -1
        with open('./select.csv', 'a', encoding='utf-8', newline='') as f:
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

                        count = len(key_res)
                        print("keyword : {0}, count: {1}".format(key_res, count))
                        if count >= 0:
                            filter_index += 1
                            writer.writerow([Issue_key, key_res])
                    print("index: {0}, filter_index: {1}".format(index, filter_index))
                    csv_file.close()
        f.close()


if __name__ == '__main__':
    source_list = ['CAS', 'HADOOP', 'HBASE', 'HDFS', 'KAFKA', 'ZK']
    #issue(source_list)
    key_issue_selector()
