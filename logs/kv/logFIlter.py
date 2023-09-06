import multiprocessing

def filter_log_chunk(chunk):
    filtered_lines = []
    valid_prefixes = ["APLY","LOG2"]
    for line in chunk:
        parts = line.split()
        if len(parts) >= 2:
            second_word = parts[1]
            if second_word in valid_prefixes:
                filtered_lines.append(line)
    return filtered_lines

def main(input_file_path, output_file_path, num_processes):
    try:
        with open(input_file_path, "r") as input_file, open(output_file_path, "w") as output_file:
            pool = multiprocessing.Pool(num_processes)

            chunk_size = 1000  # 调整此值以适应你的内存限制
            chunks = []

            for line in input_file:
                chunks.append(line)
                if len(chunks) >= chunk_size:
                    filtered_lines = pool.map(filter_log_chunk, [chunks])
                    for lines in filtered_lines:
                        output_file.writelines(lines)
                    chunks = []

            if chunks:
                filtered_lines = pool.map(filter_log_chunk, [chunks])
                for lines in filtered_lines:
                    output_file.writelines(lines)
            
            pool.close()
            pool.join()
    except FileNotFoundError:
        print("找不到输入文件。")
    except Exception as e:
        print(f"发生错误: {str(e)}")

if __name__ == "__main__":
    input_file_path = "3B_9.log"  # 替换为输入日志文件的路径
    output_file_path = "output3.log"  # 替换为输出日志文件的路径
    num_processes = 4  # 根据需要设置线程数
    main(input_file_path, output_file_path, num_processes)

