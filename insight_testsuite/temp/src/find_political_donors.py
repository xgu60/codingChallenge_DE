#coding challenge of Insight Data Engineer 
#written by Xiaodong (Sheldon) Gu
#October 29, 2017

import os
import sys
import numpy as np
import pandas as pd

#read file names from command line arguments
inputFileName = sys.argv[1]
outputFileName1 = sys.argv[2]
outputFileName2 = sys.argv[3]

#check input file path
if not os.path.exists(inputFileName):
    print "Cannot find itcont.txt file, please check the path"

#create three data frame
#data to store clean data that read from input file itcont.txt
#data_zip to store data that will write to medianvals_by_zip.txt
#data_date to store data that will write to medianvals_by_date.txt
data = pd.DataFrame()
data_zip = pd.DataFrame(columns=['CMTE_ID', 'ZIP_CODE', 'MEDIAN', 'NUM', 'TOTAL']) 
data_date = pd.DataFrame(columns=['CMTE_ID', 'DATE', 'MEDIAN', 'NUM', 'TOTAL'])

#read data from txt file, chunksize = 1 (line by line)
reader = pd.read_csv(inputFileName, sep="|", chunksize=1, header=None, 
	usecols=[0, 10, 13, 14, 15], 
	names=['CMTE_ID', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID'], 
	dtype={'ZIP_CODE': object, 'TRANSACTION_DT': object})

for line in reader:
    #ignore the line of data that 'OTHER_ID' not empty
    if not pd.isnull(line.iloc[0]['OTHER_ID']):
        continue
    
    #ignore the line of data that either 'CMTE_ID' is empty or 'TRANSACTION_AMT' is empty
    if pd.isnull(line.iloc[0]['CMTE_ID']) or pd.isnull(line.iloc[0]['TRANSACTION_AMT']):
        continue

    #append the line of data to data
    data = data.append(line)
    
    #format 'ZIP_CODE' in data.
    if pd.notnull(data.iloc[-1, 1]):
        if len(data.iloc[-1, 1]) < 5:        
            data.iloc[-1, 1] = np.nan
        else:        
            data.iloc[-1, 1] = data.iloc[-1, 1][0:5]

    #format 'TRANSACTION_DT' in data
    data.iloc[-1, 2] = pd.to_datetime(data.iloc[-1, 2], format='%m%d%Y', errors='coerce')

    #if 'ZIP_CODE' is nan, no need to calculate cummulative medianvals for zip
    if pd.isnull(data.iloc[-1]['ZIP_CODE']):
        continue

    #create sub dataframe from data that 'CMTE_ID' and 'ZIP_CODE' equal the just readin value
    df_sub_zip = data[(data.CMTE_ID == data.iloc[-1]['CMTE_ID']) & (data.ZIP_CODE == data.iloc[-1]['ZIP_CODE'])]
    #calculate mediana and store to data_zip
    data_zip.loc[data_zip.shape[0]] = [data.iloc[-1]['CMTE_ID'], 
	data.iloc[-1]['ZIP_CODE'], 
	int(round(df_sub_zip['TRANSACTION_AMT'].median())), 
	df_sub_zip.shape[0], 
	df_sub_zip['TRANSACTION_AMT'].sum()]
     
#create new dataframe that TRANSACTION_DT is valid  
df_valid_dt = data[pd.notnull(data['TRANSACTION_DT'])] 

#identify unique combinations of 'CMTE_ID', 'TRANSACTION_DT'         
df_unique_pairs = df_valid_dt.groupby(['CMTE_ID', 'TRANSACTION_DT']).size().reset_index()
#sort 'CMTE_ID', 'TRANSACTION_DT'     
df_unique_pairs = df_unique_pairs.sort_values(['CMTE_ID', 'TRANSACTION_DT'], ascending=[True, True])

for row in range (0, df_unique_pairs.shape[0]):
    #generate sub dataframe from df_valid_dt based on unique pairs
    df_sub_date = df_valid_dt[(df_valid_dt.CMTE_ID == df_unique_pairs.iloc[row]['CMTE_ID']) &
		(df_valid_dt.TRANSACTION_DT == df_unique_pairs.iloc[row]['TRANSACTION_DT'])]
    #calculate mediana and store to data_date
    data_date.loc[data_date.shape[0]] = [df_unique_pairs.iloc[row]['CMTE_ID'], 
					df_unique_pairs.iloc[row]['TRANSACTION_DT'].strftime('%m%d%Y'), 
					int(round(df_sub_date['TRANSACTION_AMT'].median())), 
					df_sub_date.shape[0], 
					df_sub_date['TRANSACTION_AMT'].sum()]

data_zip.to_csv(outputFileName1, header=None, index=None, sep='|') 
data_date.to_csv(outputFileName2, header=None, index=None, sep='|')  
#print data 
#print data_zip
#print data_date

