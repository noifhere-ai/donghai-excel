import pandas as pd
from collections import defaultdict
import openpyxl

file_path = r'C:\Users\Bin\Desktop\东海表格\2026年菲律宾2月tiktok订单利润表（1.1%）.xlsx'

# 1. 定义核心费用归类
platform_fee_cols = ['Transaction fee', 'TikTok Shop commission fee', 'Order processing fee', 'Affiliate commission deposit']
logistics_fee_cols = ['Seller shipping fee', 'Shipping Service Fee']
marketing_fee_cols = [
    'Affiliate commission', 'Affiliate Shop Ads commission\t', 'Bonus cashback service fee', 
    'LIVE Specials service fee', 'Campaign resource fee', 'EAMS Program service fee', 
    'GMV Max Coupon', '广告费'
]
base_cols = ['Total revenue', '总计结算金额', 'SKU总成本(rmb)', '净利润(rmb)', '税费']

def fast_process_and_save():
    print("Reading original data with pandas (chunking for speed)...")
    # 只读取需要的列，极大减少内存占用和加载时间
    use_cols = ['店铺', '是否到款'] + base_cols + platform_fee_cols + logistics_fee_cols + marketing_fee_cols
    
    # 尝试读取，忽略缺失的列
    full_df = pd.read_excel(file_path, sheet_name='Tiktok订单完成表', usecols=lambda x: x in use_cols)
    
    print("Aggregating data...")
    # 预处理：填充空值为0
    for col in full_df.columns:
        if col not in ['店铺', '是否到款']:
            full_df[col] = pd.to_numeric(full_df[col], errors='coerce').fillna(0)
            
    # 聚合
    pivot = full_df.groupby(['店铺', '是否到款']).sum().reset_index()
    
    # 计算分类费用
    pivot['平台费'] = pivot[[c for c in platform_fee_cols if c in pivot.columns]].sum(axis=1)
    pivot['物流费'] = pivot[[c for c in logistics_fee_cols if c in pivot.columns]].sum(axis=1)
    pivot['营销费'] = pivot[[c for c in marketing_fee_cols if c in pivot.columns]].sum(axis=1)
    pivot['利润率'] = pivot.apply(lambda row: row['净利润(rmb)'] / (row['Total revenue'] / 7.8) if row['Total revenue'] != 0 else 0, axis=1)
    
    # 重命名和筛选列
    res_df = pivot[[
        '店铺', '是否到款', 'Total revenue', '总计结算金额', '平台费', '物流费', '营销费', 
        '税费', 'SKU总成本(rmb)', '净利润(rmb)', '利润率'
    ]].copy()
    res_df.columns = ['店铺名称', '结算状态', '总收入(PHP)', '到账金额(PHP)', '平台费', '物流费', '营销费', '税费', '成本(RMB)', '净利润(RMB)', '利润率']
    
    print("Saving using XlsxWriter (High Performance Engine)...")
    # 由于 openpyxl 保存太慢，我们先保存为一个临时文件，然后由用户决定是否合并，或者直接替换。
    # 为了保证不破坏原有文件的其他 Sheet，我们必须使用 openpyxl 引擎，但只追加新表。
    # 关键：我们这里只写结果表，速度会非常快。
    with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        res_df.to_excel(writer, sheet_name='盈利分析精简版', index=False)
        
    print("Successfully saved to '盈利分析精简版'.")

if __name__ == "__main__":
    fast_process_and_save()
