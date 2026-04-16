import zipfile
import xml.etree.ElementTree as ET
import os

file_path = r'C:\Users\Bin\Desktop\东海表格\2026年菲律宾2月tiktok订单利润表（1.1%）.xlsx'

def trace_external_links(path):
    if not os.path.exists(path):
        return

    try:
        with zipfile.ZipFile(path, 'r') as zip_ref:
            # 1. 映射 Relationship ID 到文件名
            rel_map = {}
            # 读取 workbook.xml.rels 获取外部链接的 rId
            workbook_rels_path = 'xl/_rels/workbook.xml.rels'
            if workbook_rels_path in zip_ref.namelist():
                content = zip_ref.read(workbook_rels_path).decode('utf-8')
                root = ET.fromstring(content)
                ns = {'r': 'http://schemas.openxmlformats.org/package/2006/relationships'}
                for rel in root.findall('.//r:Relationship', ns):
                    if 'externalLink' in rel.get('Type'):
                        rel_map[rel.get('Id')] = rel.get('Target')

            print("--- External Link Mapping ---")
            for rid, target in rel_map.items():
                print(f"{rid} -> {os.path.basename(target)}")

            # 2. 扫描所有工作表寻找引用
            print("\n--- Scanning Sheets for External References ---")
            sheet_files = [f for f in zip_ref.namelist() if f.startswith('xl/worksheets/sheet') and f.endswith('.xml')]
            
            # 先建立 Sheet ID 到名称的映射
            sheet_name_map = {}
            workbook_xml = zip_ref.read('xl/workbook.xml').decode('utf-8')
            wb_root = ET.fromstring(workbook_xml)
            ns_main = {'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
            for s in wb_root.findall('.//main:sheet', ns_main):
                sheet_id = s.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
                sheet_name_map[f"xl/worksheets/{sheet_id}.xml"] = s.get('name')
                # 有些版本可能是 sheetN.xml 对应 rIdM
                # 简单起见，我们遍历文件名并在内容中找匹配

            for sheet_file in sheet_files:
                content = zip_ref.read(sheet_file).decode('utf-8')
                # 外部引用在 XML 中通常表现为 [1]!A1，其中 [1] 对应外部链接索引
                # 或者直接在 cell formula <f> 标签中
                found_in_sheet = False
                for rid_index in range(1, len(rel_map) + 1):
                    search_str = f"[{rid_index}]"
                    if search_str in content:
                        actual_sheet_name = sheet_name_map.get(sheet_file, sheet_file)
                        print(f"Sheet '{actual_sheet_name}' uses External Link Index {rid_index}")
                        found_in_sheet = True
                
                if not found_in_sheet:
                    # 检查是否有显式的 externalReference 标签
                    if 'externalReference' in content:
                        print(f"Sheet '{sheet_file}' contains explicit externalReference tag.")

    except Exception as e:
        print(f"Error tracing links: {e}")

trace_external_links(file_path)
