make part2
declare -a StringArray=("test/barabasi-10000.txt")
 # "test/barabasi-20000.txt" "test/barabasi-30000.txt" 
	# "test/barabasi-40000.txt" "test/barabasi-50000.txt" "test/barabasi-60000.txt" "test/barabasi-70000.txt" 
	# "test/barabasi-80000.txt" "test/barabasi-90000.txt" "test/barabasi-100000.txt" "test/bull.txt" 
	# "test/chvatal.txt" "test/coxeter.txt" "test/cubical.txt" "test/diamond.txt" "test/dodecahedral.txt" 
	# "test/erdos-10000.txt" "test/erdos-20000.txt" "test/erdos-30000.txt" "test/erdos-40000.txt" "test/erdos-50000.txt" 
	# "test/erdos-60000.txt" "test/erdos-70000.txt" "test/erdos-80000.txt" "test/erdos-90000.txt" "test/erdos-100000.txt"
	# "test/folkman.txt" "test/franklin.txt" "test/frucht.txt" "test/grotzsch.txt" "test/heawood.txt" "test/herschel.txt" "test/house.txt"
 # 	"test/housex.txt" "test/icosahedral.txt" "test/krackhardt_kite.txt" "test/levi.txt" "test/mcgee.txt" "test/meredith.txt" 
 # 	"test/noperfectmatching.txt" "test/nonline.txt" "test/octahedral.txt" "test/petersen.txt" "test/robertson.txt"
	# "test/smallestcyclicgroup.txt" "test/tetrahedral.txt" "test/thomassen.txt" "test/tutte.txt" "test/uniquely3colorable.txt"
	# "test/walther.txt" "test/zachary.txt")
 
touch outputs2.csv

for val in ${StringArray[*]}; do
	mpirun -np 2 part2 $val >> outputs2.csv
done

#./output {<file_name>} {<number of max_iterations>}