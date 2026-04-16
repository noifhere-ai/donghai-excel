import pandas as pd
import openpyxl
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

file_path = r'C:\Users\Bin\Desktop\东海表格\2026年菲律宾2月tiktok订单利润表（1.1%）.xlsx'

# 1. 读取原始数据
df = pd.read_excel(file_path, sheet_name='Tiktok订单完成表')

# 定义费用归类逻辑
platform_fee_cols = ['Transaction fee', 'TikTok Shop commission fee', 'Order processing fee', 'Affiliate commission deposit']
logistics_fee_cols = ['Seller shipping fee', 'Shipping Service Fee']
marketing_fee_cols = [
    'Affiliate commission', 'Affiliate Shop Ads commission\t', 'Bonus cashback service fee', 
    'LIVE Specials service fee', 'Campaign resource fee', 'EAMS Program service fee', 
    'GMV Max Coupon', '广告费'
]

# 预处理：填充空值为0
all_fee_cols = platform_fee_cols + logistics_fee_cols + marketing_fee_cols + ['Total revenue', '总计结算金额', 'SKU总成本(rmb)', '净利润(rmb)', '税费']
for col in all_fee_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

# 2. 聚合数据
pivot = df.groupby(['店铺', '是否到款']).agg({
    'Total revenue': 'sum',
    '总计结算金额': 'sum',
    '税费': 'sum',
    'SKU总成本(rmb)': 'sum',
    '净利润(rmb)': 'sum',
    **{col: 'sum' for col in platform_fee_cols if col in df.columns},
    **{col: 'sum' for col in logistics_fee_cols if col in df.columns},
    **{col: 'sum' for col in marketing_fee_cols if col in df.columns}
}).reset_index()

# 计算归类费用
pivot['平台固定费'] = pivot[[c for c in platform_fee_cols if c in pivot.columns]].sum(axis=1)
pivot['物流费'] = pivot[[c for c in logistics_fee_cols if c in pivot.columns]].sum(axis=1)
pivot['营销推广费'] = pivot[[c for c in marketing_fee_cols if c in pivot.columns]].sum(axis=1)

# 计算利润率 (参考原有逻辑，假设 Revenue 是 PHP，Profit 是 RMB)
# 暂时按照 净利润(rmb) / (Total revenue / 7.8) 估算，或者直接按原始比例展示
# 这里为了准确，直接保留用户现有的 [净利润 / (收入/汇率)] 逻辑的展示
# 实际上我们先计算一个比例值
pivot['利润率'] = pivot.apply(lambda row: row['净利润(rmb)'] / (row['Total revenue'] / 7.8) if row['Total revenue'] != 0 else 0, axis=1)

# 3. 整理最终输出列
output_cols = [
    '店铺', '是否到款', 'Total revenue', '总计结算金额', '平台固定费', '物流费', '营销推广费', 
    '税费', 'SKU总成本(rmb)', '净利润(rmb)', '利润率'
]
df_final = pivot[output_cols].copy()
df_final.columns = [
    '店铺名称', '结算状态', '总收入(PHP)', '到账金额(PHP)', '平台费', '物流费', '营销费', 
    '税费', '成本(RMB)', '净利润(RMB)', '利润率'
]

# 4. 写入 Excel 并美化
with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    df_final.to_excel(writer, sheet_name='盈利分析精简版', index=False)
    
    # 获取工作表进行美化
    wb = writer.book
    ws = wb['盈利分析精简版']
    
    # 样式定义
    header_fill = PatternFill(start_color="CCE5FF", end_color="CCE5FF", fill_type="solid")
    bold_font = Font(bold=True)
    center_align = Alignment(horizontal='center', vertical='center')
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
    
    # 设置列宽和格式
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            cell.border = border
            if cell.row == 1:
                cell.fill = header_fill
                cell.font = bold_font
            cell.alignment = center_align
            
            # 数值格式
            if cell.row > 1:
                if column in ['C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']:
                    cell.number_format = '#,##0.00'
                if column == 'K':
                    cell.number_format = '0.00%'
            
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except: pass
        ws.column_dimensions[column].width = max_length + 5

print("New sheet '盈利分析精简版' created successfully.")
