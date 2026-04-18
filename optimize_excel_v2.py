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
        print(f"[*] 检测到工作表: {sheets}")
        
        # 2. 逐个读取工作表（仅保留数值）
        all_data = {}
        for name in sheets:
            print(f"[*] 正在读取工作表: {name}...")
            # 使用 calamine 引擎快速读取数值
            df = pl.read_excel(input_file, sheet_name=name, engine="calamine")
            all_data[name] = df
            print(f"    - 读取成功: {len(df)} 行")

        # 3. 写入新文件（这会剔除原有的公式、超链接、冗余格式）
        print(f"[*] 正在生成精简版文件（数值化）...")
        
        # 使用 polars 批量写入多表
        with pl.ExcelWriter(output_file) as excel_writer:
            for name, df in all_data.items():
                df.write_excel(workbook=excel_writer, worksheet=name)
        
        end = time.time()
        print(f"\n[+] 优化完成！")
        print(f"[+] 原始文件大小: {input_file.stat().st_size / 1024 / 1024:.2f} MB")
        print(f"[+] 新文件大小: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
        print(f"[+] 处理总行数: {sum(len(d) for d in all_data.values())}")
        print(f"[+] 总耗时: {end - start:.2f} 秒")
        print(f"\n提示：新文件已去除了所有公式和超链接，仅保留数据。建议你同时尝试将其另存为 .xlsb 格式，速度会更快。")
        
    except Exception as e:
        print(f"\n[!] 优化过程中出错: {e}")

if __name__ == "__main__":
    optimize_all_sheets()
