@echo off 
REM ECHO

echo "################################# CHANGE THE DIRECTORY VARIABLE AS PER YOUR REQUIREMENTS IN THE SCRIPT!!!!!! #################################"

REM set the variables
SET DATADIR=C:\Users\Anvay\Desktop\DBMS

echo " %DATADIR% "

call hadoop fs -rm -r /input_dir
call hadoop fs -rm -r /output_dir
call hadoop fs -rm -r /Results
call hadoop fs -mkdir /input_dir
call hadoop fs -mkdir /output_dir
call hadoop fs -mkdir /Results

call hadoop fs -put %DATADIR%\WeatherStationLocations1.csv /input_dir 
call hadoop fs -put %DATADIR%\2006.txt /input_dir
call hadoop fs -put %DATADIR%\2007.txt /input_dir
call hadoop fs -put %DATADIR%\2008.txt /input_dir
call hadoop fs -put %DATADIR%\2009.txt /input_dir

call hadoop jar %DATADIR%\InputFileConstruction.jar InputFileConstruction /input_dir/WeatherStationLocations1.csv /input_dir/2006.txt /input_dir/2007.txt /input_dir/2008.txt /input_dir/2009.txt /output_dir
timeout /t 50 
call hadoop fs -mv /output_dir/part-r-00000 /Results/InputFileConstruction.txt
timeout /t 10 
call hadoop jar %DATADIR%\MeanTemppMonthpState.jar MeanTemppMonthpState /Results/InputFileConstruction.txt /output_dir
timeout /t 50 
call hadoop fs -mv /output_dir/part-r-00000 /Results/MeanTempOutput.txt
timeout /t 10 
call hadoop fs -rm -r /output_dir/
timeout /t 10  
call hadoop jar %DATADIR%\MonthHiLo.jar MonthHiLo /Results/MeanTempOutput.txt /output_dir
timeout /t 50
call hadoop fs -mv /output_dir/part-r-00000 /Results/MonthHiLo.txt
timeout /t 10  
call hadoop jar %DATADIR%\OutputSort.jar OutputSort /Results/MonthHiLo.txt /output_dir
timeout /t 50 
call hadoop fs -mv /output_dir/part-r-00000 /Results/OutputSort.txt 
timeout /t 10  
call hadoop fs -ls /Results 
timeout /t 10 
echo "Script has been successfully executed, MapReduce is Successfull, Check the Files in the Result Directory "
exit 0
