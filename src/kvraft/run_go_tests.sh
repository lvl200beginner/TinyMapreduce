#!/bin/bash

# 设置要执行的次数
num_runs=20

# 初始化文件名计数器
file_count=61

# 循环运行Go测试命令
for ((i=1; i<=$num_runs; i++))
do
    echo "Running Go test - Run $i"
    output_file="3A$file_count.txt"  # 构建文件名
    go test -run TestManyPartitionsManyClients3A &> "$output_file"
    ((file_count++))  # 自增文件名计数器
done

