### INFORMATION FOR THIS PROJECT

To run the project, use the command "./CS236_bl_jl.sh [path of the Location file] [path of the Recordings files] [path of the output folder]"

If anything wrong when running this script, e.g. "Permission denied" . Use the command "sudo ./CS236_bl_jl.sh [path of the Location file] [path of the Recordings files] [path of the output folder]" to get the access for this command.

Authors: 

Bocheng Li: bli147@ucr.edu

Jianqiao Liu: jliu498@ucr.edu

### OVERALL DESCRIPTION

For this project, we have two datasets, one is the locations for every station across the world, the other is the recordings of the weather from these stations. The gola is to find the states which have the most stable temperature which means we need to find the difference between the hottest month and the coldest month. Thus, we divide this project into four phase. The first phase is to find the locations in United States. The second phase is to union all the recording together. The third phase is to find the month with the lowest temperature and the highest temperature for each state. Finally, we need to calculate the difference between these two months.

### DESCRIPTION OF EACH STEP

1. The first step is to find the stations in United States. We use *map* function with replace function and split function to use the list to represent each station. Then we use *filter* function to find the station with the "CTRY" field which is the third field of the list is "US" and the "ST" field which is the fourth field is non-empty. Finally, we only keep the station ID and the state with the *map* function.
2. The second step is to union all the recordings. We combine all the RDD from the  recordings dataset into a list and use the *union* function to union all the RDD in list so that we can get the recordings from 2006 to 2009.
3. This step is to find the lowest and highest temperature for each stage. First, we use the *join* function to find the recordings of the stations in United States. Then, use *reduceByKey* function to find the minimum and the maximum temperature for each month of each stage. Next, use *groupByKey* function to find the min and max temperature among the whole year of each stage.
4. This step is to calculate the difference between the highest and the lowest temperature with the *join* function and the *map* function. It also used the *calendar* library to transform the month field. With *sortBy* function, the result can be ordered by the difference between the highest and the lowest month, in ascending order. 
5. Finally, use the *write* function to output the result in the specified format.

### TOTAL RUNTIME

Using the *time* library to calculate the runtime, the result was 129.26 seconds. 