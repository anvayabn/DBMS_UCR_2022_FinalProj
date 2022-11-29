# DBMS_UCR_2022_FinalProj
DataBase Management System | UCR | Fall 2022 | Final Project | Mapreduce 

### Team 
  - Anavaya B Narappa 
     - StudentID: 862392401 
     - EmailID: an001@ucr.edu
  
  - Manoj Nagrajappa  
     - StudentID: 862396051 
     - EmailID: mnaga024@ucr.edu 

### About 

This is the final Project for DataBase Management System (CS 236). We use MapReduce to reduce the data set to solvm e the below problem statements. 

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
- Steps 1 : Create the following directories on the Hadoop file systems.

```
hadoop fs -mkdir /input_dir
hadoop fs -mkdir /output_dir
hadoop fs -mkdir /Results

```

-Step2 
  



