##spark 学习日记 #

##RunTxt ##

读取文件（大小）,查找a个数测试：

	local(两核，8G内存，cpu平均70%)
      1g
		lines:680002,time:3551
		a-count:26390,time:41457
	  2g
		lines:1080002,time:3118
		a-count:41790,time:67709
	  10g
	  	lines:5380004,time:7660
		a-count:5309760,time:348473


     118(8g内存)
      1g
      	lines:680002,time:10673
		a-count:26390,time:30533
      2g
     	lines:1080002,time:32131
		a-count:41790,time:85542
	  10g 
	  	lines:5380004,time:14378
		a-count:5309760,time:204326
##
	2g
		lines:680002,time:10604
		a-count:26328471,time:11332
