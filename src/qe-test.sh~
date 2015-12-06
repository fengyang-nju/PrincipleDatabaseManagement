#!/bin/bash - 

cd qe
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

make clean
make

./qetest_01
./qetest_02
./qetest_03
./qetest_04
./qetest_05
./qetest_06
#./qetest_07
#./qetest_08
#./qetest_09
#./qetest_10
#./qetest_11
#./qetest_12
#./qetest_13
#./qetest_14
#./qetest_15
#./qetest_16
