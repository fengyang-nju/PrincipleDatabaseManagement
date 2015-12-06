#!/bin/bash
cd rbf
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
make clean
make
cd ../rm
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi
make clean
make
./rmtest_create_tables
#./rmtest_00
#./rmtest_01
#./rmtest_02
#./rmtest_03
#./rmtest_04
#./rmtest_05
#./rmtest_06
#./rmtest_07
#./rmtest_08
#./rmtest_09
#./rmtest_10
#./rmtest_11
#./rmtest_12
#./rmtest_13
#./rmtest_13b
#./rmtest_14
#./rmtest_15
#./rmtest_extra_1
#./rmtest_extra_2
#./rmtest_pex1
#./rmtest_pex2
./rmtest_p0
./rmtest_p1
./rmtest_p2
./rmtest_p3
./rmtest_p4
./rmtest_p5
./rmtest_p6
./rmtest_p7
./rmtest_p8
./rmtest_p9


