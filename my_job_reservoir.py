import csv
import random
import pandas as pd


#this is the code for reservoir sampling
#make an array named reservoir sampling with k lenght, put the
#first k elements into the array and then give a random number j between 0 and i 
#i is the next row and if j<k then put it in the j posistion of the array.
def reservoir_sampling_csv(file_path, k):
    reservoir = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            if i < k:
                reservoir.append(row)
            else:
                j = random.randrange(i+1)
                if j < k:
                    reservoir[j] = row
    return reservoir
csv_path2 = '/home/fneon/discogs.csv' 
csv_path = r"D:\Big_Data\discogs_20250201_releases.csv"

#total rows=17,938,222
#this is 10% k = 1,793,822.2
sampled_rows = reservoir_sampling_csv(csv_path, k=179382)

#data frames
df_sample = pd.DataFrame(sampled_rows)
df_sample.to_csv("discogs_sample_1.csv", index=False)
