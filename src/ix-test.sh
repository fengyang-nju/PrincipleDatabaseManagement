#!/bin/bash - 

unzip $1.zip
if [ $? -ne 0 ]; then
    echo "[ERROR] ZIP File Error. Please fix it!"
    echo
    exit 1
fi

cd $1
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

cd codebase
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

cd rbf
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

make clean
make

cd ../ix
if [ $? -ne 0 ]; then
    echo "[ERROR] The directory structure is not correct. Please fix it!"
    echo
    exit 1
fi

make clean
make

./ixtest_01
./ixtest_02
./ixtest_03
./ixtest_04
./ixtest_05
./ixtest_06
./ixtest_07
./ixtest_08
./ixtest_09
./ixtest_10
./ixtest_11
./ixtest_12
./ixtest_13
./ixtest_14
./ixtest_15
./ixtest_extra_01
./ixtest_extra_02

