if [ "$#" -ne 3 ]; then
    echo "./script.sh <query> <dataset> <deadline>"
    exit -1
fi
#
# Dependencies: ~/wsi_config.xml
#
FILE=~/wsi_config.xml
if [ -f $FILE ]; then
	# Get OPT_IC path
	#
	temp=$(cat ~/wsi_config.xml|grep RESOPT_HOME)
	RESOPT_HOME=$(echo $temp| awk -v FS="(>|<)" '{print $3}')
	#
	#
	# Read the config.txt folder
	#
	DATA=$(head -1 $RESOPT_HOME/config.txt)
	LUA=$(sed -n '3p' $RESOPT_HOME/config.txt)
	#
	# Generate 'on-the-fly' app1.txt
	#
	cd $DATA
	CSV_FILES=$(ls *.csv)
	cd $RESOPT_HOME
	cd $LUA
	LUA_FILENAME=$(ls *.lua)
	FILENAME=$(echo "${LUA_FILENAME%%.*}")
	LUA_MOD_FILENAME=$FILENAME'_mod.lua'
	cd $RESOPT_HOME
	echo $CSV_FILES' '$LUA_FILENAME' 'ConfigApp_1.txt' '$3 > $RESOPT_HOME/app1.txt
	#
	# Prepare the lua file with place-holder
	#
	line=$(cat $LUA$LUA_FILENAME|grep "Nodes = ")
	sed -i "s/$line/Nodes = @@nodes@@;/g" $LUA$LUA_FILENAME
	#
	# Launch OPT_IC
	#
	$RESOPT_HOME/Res_opt app1.txt f > $RESOPT_HOME/_out.txt
	#
	# Restore the original lua file
	#
	mv $LUA$LUA_MOD_FILENAME $LUA$LUA_FILENAME
	#
	# Extract requested information 
	output1=$(cat $RESOPT_HOME/_out.txt|grep "Optimum Ncores using fast optimization:" | sed -n "s/^.*:\s*\(\S*\).*$/\1/p")
	output2=$(cat $RESOPT_HOME/_out.txt|grep "N YARN containers (VMs):" | sed -n "s/^.*:\s*\(\S*\).*$/\1/p")
	echo $output1 $output2
else
   echo "FATAL ERROR: File $FILE does not exist."
fi

