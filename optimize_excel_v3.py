import polars as pl
from pathlib import Path
import time
from python_calamine import CalamineWorkbook

def optimize_all_sheets():
    input_file = Path("2月菲律宾TK副本.xlsx")
    output_file = Path("2月菲律宾TK副本_精简数值版.xlsx")
    
    print(f"[*] 正在深度优化多表文件: {input_file}")
    start = time.time()
    
    try:
        # 1. 获取所有工作表名称
        workbook = CalamineWorkbook.from_path(str(input_file))
        sheets = workbook.sheet_names
        
        # 2. 逐个读取工作表
        all_data = {}
        for name in sheets:
            # 使用 calamine 引擎快速读取数值
            df = pl.read_excel(input_file, sheet_name=name, engine="calamine")
            all_data[name] = df
            print(f"[*] 已读取 {name}: {len(df)} 行")

        # 3. 使用 polars.write_excel 的多表支持
        # 注意：在某些版本中是通过 worksheets 参数传入字典的
        print(f"[*] 正在保存到: {output_file}")
        
        # 尝试使用 worksheets 字典方式写入
        pl.write_excel(all_data, output_file)
        
        end = time.time()
        print(f"\n[+] 优化完成！")
        print(f"[+] 原始大小: {input_file.stat().st_size / 1024 / 1024:.2f} MB")
        print(f"[+] 新文件大小: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
        print(f"[+] 耗时: {end - start:.2f} 秒")
        
    except Exception as e:
        print(f"\n[!] 出错: {e}")

if __name__ == "__main__":
    optimize_all_sheets()
