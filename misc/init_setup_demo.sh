#!/bin/sh

PWD=`pwd`

# init config

cp conf/storage-schemas.conf.example conf/storage-schemas.conf
cp conf/rurouni.conf.example conf/rurouni.conf
sed -i".bak" 's?/data/kenshin?'$PWD'?g' conf/rurouni.conf

# init storage directory

mkdir -p $PWD/storage/data
mkdir -p $PWD/storage/link
mkdir -p $PWD/storage/log
mkdir -p $PWD/storage/run
