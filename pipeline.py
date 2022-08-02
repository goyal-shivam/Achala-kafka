import os
import pandas as pd
from datetime import datetime

start_time = datetime.now()

# location of pickle files
path = "/Users/thispc/Desktop/achala_dev_files/delimiter/pickle_data"

os.chdir(path)

# character comparision function
def characterComp(geohash_array,indexno):
        comp_char = [x[indexno] for x in geohash_array]
        return comp_char;

# list of dataframes
list_df = []
# iterate through all file
for file in os.listdir():
    # Check whether file is in pkl format or not
    if file.endswith(".pkl"):
        file_path = f"{path}/{file}"
        list_df.append(pd.read_pickle(file_path))

# appending all small tables reported by users into one big aggregated table
appended_df = pd.DataFrame()
for j in list_df:
    temp_df = j
    appended_df = appended_df.append(temp_df,ignore_index=True)

#list of all unique BSSID in the 
unique_bssid = appended_df.BSSID.unique()

aggregate_df = pd.DataFrame() # df with geohash after conflict resolution

# conflict resolution for each BSSID
for k in unique_bssid:
    bssid = k
    print(bssid)
    df_input = [] # df for each unique BSSID
    geohash_input = [] # geohash input for conflict resolution
    df_input = appended_df.loc[(appended_df['BSSID'] == bssid)] 
    geohash_input = df_input['geohash']
    res0 = geohash_input.to_numpy() # convert from series to an array
    
    # sorting algorithm
    A = res0

    # Traverse through all array elements
    for i in range(len(A)):
      # Find the minimum element in remaining unsorted array
      min_idx = i
      
      for j in range(i+1, len(A)):
          if A[min_idx] > A[j]:
              min_idx = j
      # Swap the found minimum element with the first element
      A[i], A[min_idx] = A[min_idx], A[i]

    res1 = A # sorted geohash values
    
    indno=0
    while indno<12 :
            answer = characterComp(res1,indno)
            print(answer)
            res = {}
            
            for i in answer:
                res[i] = answer.count(i) # count of character
                
            print(res)
            
            if all(value == 1 for value in res.values()): # breaks loop and returns least character
                break
           
            new_value = max(res, key=res.get)
            print("Highest value from dictionary:",new_value)
            res1 = [idx for idx in res1 if idx[indno].lower() == new_value.lower()]
            print("The list of matching character geohash:" + str(res1))
            indno+=1
            
    print("conflict resoluted geohash:",res1[0])
    aggregate_df = aggregate_df.append(appended_df.loc[(appended_df['BSSID'] == bssid) & (appended_df['geohash'] == res1[0])])

print(aggregate_df)
end_time = datetime.now()
print(end_time - start_time)
