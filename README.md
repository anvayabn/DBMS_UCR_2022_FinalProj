# DBMS_UCR_2022_FinalProject 
DataBase Management System | UCR | Fall 2022 | Final Project | Mapreduce 

### Team 
  - Anvaya B Narappa 
     - StudentID: 862392401 
     - EmailID: an001@ucr.edu
  
  - Manoj Nagrajappa  
     - StudentID: 862396051 
     - EmailID: mnaga024@ucr.edu 

### About 

This is the final Project for DataBase Management System (CS 236). We use MapReduce to reduce the data set to solve the below problem statements. 

### Problem Statements 

1.	For stations within the United States, group the stations by state. For each state with readings
a.	find the average temperature recorded for each month (ignoring year)
2.	Find the months with the highest and lowest averages for that state. Order the states by the difference between the highest and lowest month average, ascending.
For each state, return:
a.	The state abbreviation, e.g., “CA”
b.	The average temperature and name of the highest month, e.g., “90, July”
c.	The average temperature and name of the lowest month, e.g., “50, January”
d.	The difference between the two (from 2.b and 2.c), e.g., “40”
3.	Order the states by the difference, ascending. 
a.	Each row of your output should contain: The state abbreviation, the average temperature and name of the highest month, the average temperature and name of the lowest month and the difference between the two.

### Steps
- Step 0 : Use the script ```mapReduceScriptRev.cmd``` to execute all he map reduce jobs at once.

  - **Please Note** that, this script is designed for windows cmd or powershell. If the the script does not run you can follow the steps below one after the other.Also    change the DATADIR variable in the script as per the path in which the datasets and the jar files are stored. If the mapreduce jobs are slow please increase/decrease the timeout in the script as per requirement. 
  - **This will delete existing input, ouput directories, Be Carefull**
  - After Successfull Execution the following will be displayed **" Script has been successfully executed, Mapreduce is Successfull, Check the Files in the Result Directory "**
  
  To execute the script, do the following with admin rights on the cmd/powershell:
```
.\mapReduceScriptRev.cmd

```
   
  
- Step 1 : Create the following directories on the Hadoop file systems.

```
hadoop fs -mkdir /input_dir
hadoop fs -mkdir /output_dir
hadoop fs -mkdir /Results

```

- Step 2 : Copy the CSV file and the datasets to the input directory 

```
hadoop fs -put C:\Users\Anvay\Desktop\DBMS\WeatherStationLocations.csv /input_dir 
hadoop fs -put C:\Users\Anvay\Desktop\DBMS\2006.txt /input_dir 
hadoop fs -put C:\Users\Anvay\Desktop\DBMS\2007.txt /input_dir 
hadoop fs -put C:\Users\Anvay\Desktop\DBMS\2008.txt /input_dir 
hadoop fs -put C:\Users\Anvay\Desktop\DBMS\2009.txt /input_dir 
  
```
- Step 3 : Construct the Input File. The input file is used for further MapReduce jobs. Use the InputFileConstruction.jar to do the same. Move the file from output directory to Results Folder
```
hadoop jar C:\Users\Anvay\Desktop\DBMS\InputFileConstruction.jar InputFileConstruction /input_dir/WeatherStationLocations1.csv /input_dir/2006.txt /input_dir/2007.txt /input_dir/2008.txt /input_dir/2009.txt /output_dir

hadoop fs -mv /output_dir/part-r-00000 /Results/InputFileConstruction.txt
```

- Step 4 : Use the InputFileConstruction.txt obtained in the previous job to find the Mean Temperature per month per state. Use the MeanTemppMonthpState.jar, and move the output file to the Results folder. 

```
hadoop jar C:\Users\Anvay\Desktop\DBMS\MeanTemppMonthpState.jar MeanTemppMonthpState /Results/InputFileConstruction.txt /output_dir

hadoop fs -mv /output_dir/part-r-00000 /Results/MeanTempOutput.txt

```

- Step 5 : To find the month with highest and lowest average use the jar, MonthHiLo.jar. Before executing the jar, please remember to delete the /output_dir, after execution move the result to Result folder. 

```
hadoop fs -rm -r /output_dir/
hadoop jar C:\Users\Anvay\Desktop\DBMS\MonthHiLo.jar MonthHiLo /Results/MeanTempOutput.txt /output_dir
hadoop fs -mv /output_dir/part-r-00000 /Results/MonthHiLo.txt

```

- Step 6 : Sort the output of the previous result using OutputSort.jar. Move the output file to Result Folder. 

```
hadoop jar C:\Users\Anvay\Desktop\DBMS\OutputSort.jar OutputSort /Results/MonthHiLo.txt /output_dir
hadoop fs -mv /output_dir/part-r-00000 /Results/OutputSort.txt 

```


