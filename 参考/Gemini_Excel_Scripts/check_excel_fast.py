import zipfile
import xml.etree.ElementTree as ET
import os

file_path = r'C:\Users\Bin\Desktop\东海表格\2026年菲律宾2月tiktok订单利润表（1.1%）.xlsx'

def analyze_xlsx_internals(path):
    if not os.path.exists(path):
        print(f"Error: File not found at {path}")
        return

    try:
        with zipfile.ZipFile(path, 'r') as zip_ref:
            # 1. 检查外部链接关系
            print("--- Checking External Links (rels) ---")
            external_links_found = False
            for file_info in zip_ref.infolist():
                if 'externalLink' in file_info.filename and file_info.filename.endswith('.rels'):
                    content = zip_ref.read(file_info.filename).decode('utf-8')
                    print(f"External Relationship in {file_info.filename}:")
                    print(content)
                    external_links_found = True
            
            if not external_links_found:
                print("No external link relationships found in file structure.")

            # 2. 检查数据透视表缓存定义 (Pivot Cache Definitions)
            print("\n--- Checking Pivot Table Cache Sources ---")
            pivot_cache_files = [f for f in zip_ref.namelist() if 'pivotCacheDefinition' in f]
            for cache_file in pivot_cache_files:
                print(f"Analyzing {cache_file}...")
                content = zip_ref.read(cache_file).decode('utf-8')
                root = ET.fromstring(content)
                
                # 寻找 worksheetSource (内部) 或 externalReference (外部)
                # XML 命名空间通常是 http://schemas.openxmlformats.org/spreadsheetml/2006/main
                ns = {'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
                
                cache_source = root.find('.//main:cacheSource', ns)
                if cache_source is not None:
                    source_type = cache_source.get('type')
                    print(f"  Cache Source Type: {source_type}")
                    
                    ws_source = cache_source.find('.//main:worksheetSource', ns)
                    if ws_source is not None:
                        sheet = ws_source.get('sheet')
                        ref = ws_source.get('ref')
                        print(f"  Internal Source -> Sheet: {sheet}, Range: {ref}")
                    
                    ext_ref = cache_source.find('.//main:externalReference', ns)
                    if ext_ref is not None:
                        rid = ext_ref.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
                        print(f"  WARNING: External Source detected (Relationship ID: {rid})")

    except Exception as e:
        print(f"Error analyzing file: {e}")

analyze_xlsx_internals(file_path)
