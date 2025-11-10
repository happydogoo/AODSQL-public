import random

def generate_inserts(filename, table_name, name, start_id, count):
    with open(filename, 'a', encoding='utf-8') as f:
        for i in range(start_id, start_id + count):
            age = random.randint(18, 25)
            gpa = round(random.uniform(2.0, 4.0), 1)
            sql = f"INSERT INTO {table_name} VALUES ({i}, '{name}', {age}, {gpa});\n"
            f.write(sql)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="批量生成INSERT语句到SQL文件")
    parser.add_argument('--file', type=str, default='sql/demo/demo2.sql', help='SQL文件路径')
    parser.add_argument('--table', type=str, default='students', help='表名')
    parser.add_argument('--name', type=str, default='Alice', help='名字')
    parser.add_argument('--start', type=int, default=1, help='起始ID')
    parser.add_argument('--count', type=int, default=10000, help='插入条数')
    args = parser.parse_args()

    generate_inserts(args.file, args.table, args.name, args.start, args.count)
    print(f"已向 {args.file} 追加写入 {args.count} 条INSERT语句。")