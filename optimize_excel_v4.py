import polars as pl
from pathlib import Path
import time
from python_calamine import CalamineWorkbook
import xlsxwriter

def optimize_all_sheets():
    input_file = Path("2月菲律宾TK副本.xlsx")
    output_file = Path("2月菲律宾TK副本_精简数值版.xlsx")
    
    print(f"[*] 正在深度优化多表文件: {input_file}")
    start = time.time()
    
    try:
        # 1. 获取所有工作表名称
        workbook_info = CalamineWorkbook.from_path(str(input_file))
        sheets = workbook_info.sheet_names
        
        # 2. 写入新文件
        print(f"[*] 正在创建新文件并写入数据...")
        with xlsxwriter.Workbook(str(output_file)) as workbook:
            for name in sheets:
                print(f"[*] 正在处理工作表: {name}...")
                # 读取数值
                df = pl.read_excel(input_file, sheet_name=name, engine="calamine")
                # 写入数值（这会自然丢弃公式和链接）
                df.write_excel(workbook=workbook, worksheet=name)
                print(f"    - 已写入 {len(df)} 行")

        end = time.time()
        print(f"\n[+] 优化成功！")
        print(f"[+] 原始文件大小: {input_file.stat().st_size / 1024 / 1024:.2f} MB")
        print(f"[+] 新文件大小: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
        print(f"[+] 耗时: {end - start:.2f} 秒")
        
    except Exception as e:
        print(f"\n[!] 优化过程中出错: {e}")

if __name__ == "__main__":
    optimize_all_sheets()
