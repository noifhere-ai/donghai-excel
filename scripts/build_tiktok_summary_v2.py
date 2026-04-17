import os
import re
import time
from pathlib import Path
import polars as pl
from tqdm import tqdm

# ==========================================
# 1. 配置与常量定义
# ==========================================

ORDER_HEADERS = [
    "店铺", "Order ID", "是否到款", "Order Status", "Order Substatus",
    "Cancelation/Return Type", "Normal or Pre-order", "SKU ID", "Seller SKU",
    "Product Name", "Variation", "Quantity", "Sku Quantity of return",
    "SKU Unit Original Price", "SKU Subtotal Before Discount", "SKU Platform Discount",
    "SKU Seller Discount", "SKU Subtotal After Discount", "Shipping Fee After Discount",
    "Original Shipping Fee", "Shipping Fee Seller Discount", "Shipping Fee Platform Discount",
    "Payment platform discount", "Taxes", "Order Amount", "Order Refund Amount",
    "Created Time", "Paid Time", "RTS Time", "Shipped Time", "Delivered Time",
    "Cancelled Time", "Cancel By", "Cancel Reason", "Fulfillment Type",
    "Warehouse Name", "Tracking ID", "Delivery Option", "Shipping Provider Name",
    "Buyer Message", "Buyer Username", "Recipient", "Phone #", "Country",
    "Region", "Province", "Municipality", "Barangay", "Detail Address",
    "Additional address information", "Payment Method", "Weight(kg)",
    "Product Category", "Package ID", "Seller Note", "Checked Status",
    "Checked Marked by", "Total revenue", "SKU成本(rmb)", "SKU总成本(rmb)",
    "Transaction fee", "TikTok Shop commission fee", "Seller shipping fee",
    "Affiliate commission", "Affiliate Shop Ads commission", "Order processing fee",
    "Bonus cashback service fee", "Affiliate commission deposit", "Shipping Service Fee",
    "Voucher Xtra Service Fee", "Pre-order service fee", "Affiliate partner commission",
    "LIVE Specials service fee", "Campaign resource fee", "EAMS Program service fee",
    "GMV Max Coupon", "广告费", "Total fees", "结算金额", "税费", "总计结算金额",
    "净利润(rmb)", "备注", "取消订单", "退款原因", "二次销售(0/1)"
]

FEE_FIELDS = [
    "Transaction fee", "TikTok Shop commission fee", "Seller shipping fee",
    "Affiliate commission", "Affiliate Shop Ads commission", "Order processing fee",
    "Bonus cashback service fee", "Affiliate commission deposit", "Shipping Service Fee",
    "Voucher Xtra Service Fee", "Pre-order service fee", "Affiliate partner commission",
    "LIVE Specials service fee", "Campaign resource fee", "EAMS Program service fee",
    "GMV Max Coupon"
]

# ==========================================
# 2. 工具函数
# ==========================================

EXPECTED_COLS = ORDER_HEADERS + FEE_FIELDS + ["Total revenue", "Total settlement amount", "Adjusted amount", "Type", "Related ID"]
LOWER_TO_EXPECTED = {c.lower(): c for c in EXPECTED_COLS}
LOWER_TO_EXPECTED["order/adjustment id"] = "Order ID"
LOWER_TO_EXPECTED["order id"] = "Order ID"
LOWER_TO_EXPECTED["related order id"] = "Related ID"

def extract_month(name: str) -> int:
    match = re.search(r"(\d+)月", name)
    return int(match.group(1)) if match else 2

def get_store_info(path: Path):
    month = extract_month(path.name)
    parts = path.parts
    owner = "未知"
    category = ""
    for p in reversed(parts):
        if "阳玲" in p: owner = "阳玲"
        if "黄海镕" in p: owner = "黄海镕"
        if "刘林长" in p: owner = "刘林长"
        if "廖楠" in p: owner = "廖楠"
        if "徐鑫权" in p: owner = "徐鑫权"
        
        if "母婴" in p: category = "母婴"
        if "品牌美妆" in p: category = "品牌美妆"
        if "汽配" in p: category = "汽配"
        if "3C" in p: category = "3C"
        if "小家电" in p: category = "小家电"
        if "工具" in p: category = "工具"
        if "户外" in p: category = "户外"
        
    if owner == "未知" and category in ["母婴", "品牌美妆", "汽配"]:
        owner = "阳玲"
        
    return f"{month}月份-{owner}{category}"

# ==========================================
# 3. 核心执行引擎
# ==========================================

def run_reconstruction():
    start_time = time.time()
    workspace = Path("c:/Users/Bin/Desktop/东海表格")
    src_dir = workspace / "01-TK文件/tk/011-系统数据"
    output_dir = workspace / "01-TK文件/程序生成结果"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[*] 开始扫描数据源: {src_dir}")
    
    orders_files = list(src_dir.rglob("*订单*.xlsx")) + list(src_dir.rglob("*Orders*.xlsx"))
    payout_files = list(src_dir.rglob("*回款*.xlsx")) + list(src_dir.rglob("*Settlement*.xlsx"))
    cost_file = workspace / "成本表.xlsx"

    # 3.1 读取订单数据
    all_orders = []
    for f in tqdm(orders_files, desc="读取订单表"):
        if "营销费" in f.name or "退款" in f.name or f.name.startswith("~$"):
            continue
        store = get_store_info(f)
        try:
            df = pl.read_excel(f, engine="calamine")
            mapping = {}
            for col in df.columns:
                cleaned = col.strip()
                low = cleaned.lower()
                if low in LOWER_TO_EXPECTED:
                    mapping[col] = LOWER_TO_EXPECTED[low]
                elif low == "order status": 
                    mapping[col] = "Order Status"
                elif low == "seller sku": 
                    mapping[col] = "Seller SKU"
                else: 
                    mapping[col] = cleaned
            df = df.rename(mapping)
            
            df = df.with_columns([
                pl.col(c).cast(pl.Utf8) for c in df.columns if "ID" in c or "SKU" in c or "Time" in c
            ]).with_columns([
                pl.lit(store).alias("店铺"),
                pl.col("Order ID").cast(pl.Utf8).str.strip_chars().alias("Order ID")
            ])
            if "Order Status" in df.columns:
                df = df.filter(~pl.col("Order Status").is_in(["Canceled", "Cancelled"]))
            all_orders.append(df)
        except Exception as e:
            print(f"跳过异常订单文件 {f.name}: {e}")
            
    if not all_orders:
        print("错误: 未能读取到任何有效的订单数据！")
        return

    df_orders = pl.concat(all_orders, how="diagonal_relaxed")
    print(f"[+] 订单加载完成，共 {len(df_orders)} 行")

    # 3.2 读取回款数据
    all_payouts = []
    for f in tqdm(payout_files, desc="读取回款表"):
        if f.name.startswith("~$"):
            continue
        store = get_store_info(f)
        try:
            df = pl.read_excel(f, engine="calamine")
            mapping = {}
            for col in df.columns:
                cleaned = col.strip()
                low = cleaned.lower()
                if low in LOWER_TO_EXPECTED:
                    mapping[col] = LOWER_TO_EXPECTED[low]
                else: 
                    mapping[col] = cleaned
            df = df.rename(mapping)
            
            numeric_cols = [c for c in FEE_FIELDS + ["Total revenue", "Total settlement amount", "Adjusted amount"] if c in df.columns]
            
            df = df.with_columns([
                pl.col(c).cast(pl.Utf8) for c in df.columns if "ID" in c or "Type" in c
            ]).with_columns([
                pl.col(c).cast(pl.Float64, strict=False).fill_null(0) for c in numeric_cols
            ]).with_columns([
                pl.lit(store).alias("店铺"),
                pl.col("Order ID").cast(pl.Utf8).str.strip_chars().alias("Order ID") if "Order ID" in df.columns else pl.lit(None).cast(pl.Utf8).alias("Order ID"),
                pl.col("Related ID").cast(pl.Utf8).str.strip_chars().alias("Related ID") if "Related ID" in df.columns else pl.lit(None).cast(pl.Utf8).alias("Related ID")
            ])
            all_payouts.append(df)
        except Exception as e:
            print(f"跳过异常回款文件 {f.name}: {e}")
            
    if not all_payouts:
        print("错误: 未能读取到任何有效的回款数据！")
        return

    df_payout_raw = pl.concat(all_payouts, how="diagonal_relaxed")
    
    available_cols = set(df_payout_raw.columns)
    fee_cols_to_sum = [f for f in FEE_FIELDS if f in available_cols]
    
    # 回款聚合准备
    df_payout_agg = df_payout_raw.select([
        pl.coalesce(["Related ID", "Order ID"]).alias("Match ID"),
        pl.col("Total revenue").cast(pl.Float64, strict=False).fill_null(0).alias("Total revenue"),
        *[pl.col(f).cast(pl.Float64, strict=False).fill_null(0) for f in fee_cols_to_sum],
        pl.col("Type")
    ]).filter(
        pl.col("Match ID").is_not_null()
    )

    # 剔除广告单独做汇总
    df_payout_agg_regular = df_payout_agg.filter(
        pl.col("Type") != "GMV Payment for TikTok Ads"
    ).drop("Type").group_by("Match ID").sum()

    # 此处为自动调整行生成
    adj_rows = []
    matched_adj = df_payout_raw.filter(pl.col("Type") == "GMV Payment for TikTok Ads")
    if not matched_adj.is_empty():
        # 汇总广告金额，一般记在 Total settlement amount
        sum_col = "Total settlement amount" if "Total settlement amount" in matched_adj.columns else "Total revenue"
        summed = matched_adj.group_by("店铺").agg([
            pl.col(sum_col).sum().alias("Amount")
        ])
        for row in summed.to_dicts():
            adj_rows.append({
                "店铺": row["店铺"],
                "Order ID": "SYS-ADJ-广告",
                "是否到款": "是",
                "广告费": row["Amount"],
                "Quantity": 0,
                "备注": "手动归集的线上广告费支出"
            })
            
    # 读取成本表
    df_costs = pl.read_excel(cost_file, engine="calamine").select([
        pl.col("编号").cast(pl.Utf8).alias("Seller SKU"),
        pl.col("总成本").alias("Unit Cost")
    ])

    # 3.4 数据合并
    df_final = df_orders.join(
        df_payout_agg_regular, left_on="Order ID", right_on="Match ID", how="left"
    ).join(
        df_costs, on="Seller SKU", how="left"
    )

    # 在计算前追加自动调整行（广告），确保适用所有公式计算
    if adj_rows:
        df_adj = pl.DataFrame(adj_rows)
        for col in df_final.columns:
            if col not in df_adj.columns:
                df_adj = df_adj.with_columns(pl.lit(None).alias(col))
        df_adj = df_adj.select(df_final.columns)
        df_final = pl.concat([df_final, df_adj], how="diagonal_relaxed")

    # 3.5 计算字段 补齐核心列
    if "是否到款" not in df_final.columns:
        df_final = df_final.with_columns(pl.lit(None).alias("是否到款"))
    if "广告费" not in df_final.columns:
        df_final = df_final.with_columns(pl.lit(0.0).alias("广告费"))
    if "Quantity" not in df_final.columns:
        df_final = df_final.with_columns(pl.lit(0).alias("Quantity"))

    # 设置默认汇率与税率
    exchange_rate = 8.4672
    tax_rate = 0.011

    df_final = df_final.with_columns([
        pl.col("Quantity").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("Unit Cost").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("Total revenue").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("广告费").cast(pl.Float64, strict=False).fill_null(0),
    ])

    df_final = df_final.with_columns([
        pl.when(pl.col("是否到款").is_null())
          .then(pl.when((pl.col("Total revenue") != 0) | (pl.col("广告费") != 0)).then(pl.lit("是")).otherwise(pl.lit("否")))
          .otherwise(pl.col("是否到款"))
          .alias("是否到款"),
        (pl.col("Quantity") * pl.col("Unit Cost")).alias("SKU总成本(rmb)"),
        pl.col("Unit Cost").alias("SKU成本(rmb)")
    ])

    # 补齐 FEE_FIELDS 以作聚合计算
    for f in FEE_FIELDS:
        if f not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(0.0).alias(f))
        else:
            df_final = df_final.with_columns(pl.col(f).fill_null(0.0))
            
    df_final = df_final.with_columns([
        (pl.sum_horizontal(FEE_FIELDS) + pl.col("广告费")).alias("Total fees"),
    ]).with_columns([
        (pl.col("Total revenue") + pl.col("Total fees")).alias("结算金额")
    ]).with_columns([
        (pl.col("结算金额") * tax_rate).alias("税费")
    ]).with_columns([
        (pl.col("结算金额") - pl.col("税费")).alias("总计结算金额")
    ]).with_columns([
        (pl.col("总计结算金额") / exchange_rate - pl.col("SKU总成本(rmb)")).alias("净利润(rmb)")
    ])

    # 3.6 补齐全量模板字段，严格保持顺序
    for col in ORDER_HEADERS:
        if col not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None).alias(col))
            
    df_final = df_final.select(ORDER_HEADERS)

    # 3.7 文件输出
    output_file = output_dir / "Tiktok订单完成表.xlsx"
    print(f"[*] 正在写入结果文件: {output_file}")
    
    try:
        df_final.write_excel(output_file)
    except Exception as e:
        print(f"写入 Excel 失败, 将尝试使用其它引擎或保存为 CSV: {e}")
        df_final.write_csv(output_file.with_suffix(".csv"))
        print(f"已保底输出 CSV: {output_file.with_suffix('.csv')}")

    end_time = time.time()
    print(f"[*] 处理完成！耗时: {end_time - start_time:.2f} 秒")

if __name__ == "__main__":
    run_reconstruction()
