from __future__ import annotations

import argparse
import math
import re
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


ORDER_EXTRA_FEE_FIELDS = [
    "Transaction fee",
    "TikTok Shop commission fee",
    "Seller shipping fee",
    "Affiliate commission",
    "Affiliate Shop Ads commission",
    "Order processing fee",
    "Bonus cashback service fee",
    "Affiliate commission deposit",
    "Shipping Service Fee",
    "Voucher Xtra Service Fee",
    "Pre-order service fee",
    "Affiliate partner commission",
    "LIVE Specials service fee",
    "Campaign resource fee",
    "EAMS Program service fee",
    "GMV Max Coupon",
]

ORDER_HEADERS = [
    "店铺",
    "Order ID",
    "Order ID",
    "是否到款",
    "Order Status",
    "Order Substatus",
    "Cancelation/Return Type",
    "Normal or Pre-order",
    "SKU ID",
    "Seller SKU",
    "Product Name",
    "Variation",
    "Quantity",
    "Sku Quantity of return",
    "SKU Unit Original Price",
    "SKU Subtotal Before Discount",
    "SKU Platform Discount",
    "SKU Seller Discount",
    "SKU Subtotal After Discount",
    "Shipping Fee After Discount",
    "Original Shipping Fee",
    "Shipping Fee Seller Discount",
    "Shipping Fee Platform Discount",
    "Payment platform discount",
    "Taxes",
    "Order Amount",
    "Order Refund Amount",
    "Created Time",
    "Paid Time",
    "RTS Time",
    "Shipped Time",
    "Delivered Time",
    "Cancelled Time",
    "Cancel By",
    "Cancel Reason",
    "Fulfillment Type",
    "Warehouse Name",
    "Tracking ID",
    "Delivery Option",
    "Shipping Provider Name",
    "Buyer Message",
    "Buyer Username",
    "Recipient",
    "Phone #",
    "Country",
    "Region",
    "Province",
    "Municipality",
    "Barangay",
    "Detail Address",
    "Additional address information",
    "Payment Method",
    "Weight(kg)",
    "Product Category",
    "Package ID",
    "Seller Note",
    "Checked Status",
    "Checked Marked by",
    "Total revenue",
    "SKU成本(rmb)",
    "SKU总成本(rmb)",
    "Transaction fee",
    "TikTok Shop commission fee",
    "Seller shipping fee",
    "Affiliate commission",
    "Affiliate Shop Ads commission",
    "Order processing fee",
    "Bonus cashback service fee",
    "Affiliate commission deposit",
    "Shipping Service Fee",
    "Voucher Xtra Service Fee",
    "Pre-order service fee",
    "Affiliate partner commission",
    "LIVE Specials service fee",
    "Campaign resource fee",
    "EAMS Program service fee",
    "GMV Max Coupon",
    "广告费",
    "Total fees",
    "结算金额",
    "税费",
    "总计结算金额",
    "净利润(rmb)",
    "备注",
    "取消订单",
    "退款原因",
    "二次销售(0/1)",
]

PAYOUT_HEADERS = [
    "店铺",
    "Order/adjustment ID",
    "Type",
    "Order created time",
    "Order settled time",
    "Currency",
    "Total settlement amount",
    "Total revenue",
    "Subtotal after seller discounts",
    "Subtotal before discounts",
    "Seller discounts",
    "Refund subtotal after seller discounts",
    "Refund subtotal before seller discounts",
    "Refund of seller discounts",
    "Total fees",
    "Transaction fee",
    "TikTok Shop commission fee",
    "Seller shipping fee",
    "Actual shipping fee",
    "Platform shipping fee discount",
    "Customer shipping fee",
    "Refund customer shipping fee",
    "Actual return shipping fee",
    "Shipping fee subsidy",
    "Affiliate commission",
    "Affiliate partner commission",
    "Affiliate Shop Ads commission",
    "Affiliate commission deposit",
    "Affiliate commission refund",
    "Affiliate Partner shop ads commission",
    "GST",
    "SFP service fee",
    "Bonus cashback service fee",
    "LIVE Specials service fee",
    "Voucher Xtra service fee",
    "EAMS Program service fee",
    "Flash Sale service fee",
    "TikTok PayLater program fee",
    "Shipping service fee",
    "Campaign resource fee",
    "Order processing fee",
    "Pre-order service fee",
    "GMV Max Coupon",
    "Ajustment amount",
    "Related order ID",
    "Customer payment",
    "Customer refund",
    "Seller co-funded voucher discount",
    "Refund of seller co-funded voucher discount",
    "Platform discounts",
    "Refund of platform discounts",
    "Platform co-funded voucher discounts",
    "Refund of platform co-funded voucher discounts",
    "Seller shipping fee discount",
    "Estimated package weight (g)",
    "Actual package weight (g)",
]

PAYOUT_FIELD_ALIASES = {
    "Total revenue": "Total revenue",
    "Transaction fee": "Transaction fee",
    "TikTok Shop commission fee": "TikTok Shop commission fee",
    "Seller shipping fee": "Seller shipping fee",
    "Affiliate commission": "Affiliate commission",
    "Affiliate Shop Ads commission": "Affiliate Shop Ads commission",
    "Order processing fee": "Order processing fee",
    "Bonus cashback service fee": "Bonus cashback service fee",
    "Affiliate commission deposit": "Affiliate commission deposit",
    "Shipping Service Fee": "Shipping service fee",
    "Voucher Xtra Service Fee": "Voucher Xtra service fee",
    "Pre-order service fee": "Pre-order service fee",
    "Affiliate partner commission": "Affiliate partner commission",
    "LIVE Specials service fee": "LIVE Specials service fee",
    "Campaign resource fee": "Campaign resource fee",
    "EAMS Program service fee": "EAMS Program service fee",
    "GMV Max Coupon": "GMV Max Coupon",
}

ANALYSIS_HEADERS = [
    "店铺",
    "是否到款",
    "Quantity",
    "Sku Quantity of return",
    "SKU Unit Original Price",
    "SKU Subtotal Before Discount",
    "SKU Platform Discount",
    "SKU Seller Discount",
    "SKU Subtotal After Discount",
    "Shipping Fee After Discount",
    "Original Shipping Fee",
    "Shipping Fee Seller Discount",
    "Shipping Fee Platform Discount",
    "Payment platform discount",
    "Taxes",
    "Order Amount",
    "Order Refund Amount",
    "Cancelled Time",
    "Weight(kg)",
    "Package ID",
    "Seller Note",
    "Total revenue",
    "SKU成本(rmb)",
    "SKU总成本(rmb)",
    "Transaction fee",
    "TikTok Shop commission fee",
    "Seller shipping fee",
    "Affiliate commission",
    "Affiliate Shop Ads commission\t",
    "Order processing fee",
    "Bonus cashback service fee",
    "Affiliate commission deposit",
    "Shipping Service Fee",
    "Voucher Xtra Service Fee",
    "Pre-order service fee",
    "Affiliate partner commission",
    "LIVE Specials service fee",
    "Campaign resource fee",
    "EAMS Program service fee",
    "GMV Max Coupon",
    "广告费",
    "Total fees",
    "结算金额",
    "税费",
    "总计结算金额",
    "总收入(RMB)",
    "净利润(rmb)",
    "利润率",
    "取消订单",
    "Order ID",
    "Order ID.1",
    "SKU ID",
]

AD_IMPORT_SHEET_NAME = "广告费导入表"
AD_IMPORT_GUIDE_SHEET_NAME = "填写说明"
GENERATED_DIR_NAME = "程序生成结果"
AD_IMPORT_HEADERS = [
    "启用",
    "记录类型",
    "月份",
    "店铺",
    "来源单据",
    "来源行号",
    "分摊金额(PHP)",
    "分摊方式",
    "筛选_是否到款",
    "筛选_Order Status",
    "筛选_Seller SKU",
    "筛选_开始时间",
    "筛选_结束时间",
    "调整_Order ID",
    "调整_是否到款",
    "调整_Order Status",
    "调整_Seller SKU",
    "调整_Quantity",
    "Total revenue",
    "Transaction fee",
    "TikTok Shop commission fee",
    "Seller shipping fee",
    "Affiliate commission",
    "Affiliate Shop Ads commission",
    "Order processing fee",
    "Bonus cashback service fee",
    "Affiliate commission deposit",
    "Shipping Service Fee",
    "Voucher Xtra Service Fee",
    "Pre-order service fee",
    "Affiliate partner commission",
    "LIVE Specials service fee",
    "Campaign resource fee",
    "EAMS Program service fee",
    "GMV Max Coupon",
    "广告费",
    "覆盖_Total fees",
    "覆盖_结算金额",
    "覆盖_税费",
    "覆盖_总计结算金额",
    "覆盖_净利润(rmb)",
    "备注",
    "退款原因",
    "二次销售(0/1)",
]
AD_IMPORT_ALLOCATION_TYPES = {"分摊广告费"}
AD_IMPORT_ADJUSTMENT_TYPES = {"新增调整行"}
AD_ALLOCATION_METHODS = {
    "按订单金额": "Order Amount",
    "按Total revenue": "Total revenue",
    "按结算金额": "结算金额",
    "按数量": "Quantity",
    "平均到订单": None,
}

AD_IMPORT_ALWAYS_REQUIRED_HEADERS = {"启用", "记录类型", "月份", "店铺", "来源单据"}
AD_IMPORT_ALLOCATION_REQUIRED_HEADERS = {"分摊金额(PHP)", "分摊方式"}
AD_IMPORT_ALLOCATION_OPTIONAL_HEADERS = {"来源行号", "筛选_是否到款", "筛选_Order Status", "筛选_Seller SKU", "筛选_开始时间", "筛选_结束时间", "备注"}
AD_IMPORT_ADJUSTMENT_REQUIRED_HEADERS = set()
AD_IMPORT_ADJUSTMENT_OPTIONAL_HEADERS = {
    "来源行号",
    "调整_Order ID",
    "调整_是否到款",
    "调整_Order Status",
    "调整_Seller SKU",
    "调整_Quantity",
    "Total revenue",
    "Transaction fee",
    "TikTok Shop commission fee",
    "Seller shipping fee",
    "Affiliate commission",
    "Affiliate Shop Ads commission",
    "Order processing fee",
    "Bonus cashback service fee",
    "Affiliate commission deposit",
    "Shipping Service Fee",
    "Voucher Xtra Service Fee",
    "Pre-order service fee",
    "Affiliate partner commission",
    "LIVE Specials service fee",
    "Campaign resource fee",
    "EAMS Program service fee",
    "GMV Max Coupon",
    "广告费",
    "覆盖_Total fees",
    "覆盖_结算金额",
    "覆盖_税费",
    "覆盖_总计结算金额",
    "覆盖_净利润(rmb)",
    "备注",
    "退款原因",
    "二次销售(0/1)",
}

ORDER_PRIMARY_ID_INDEX = 1
ORDER_SECONDARY_ID_INDEX = 2
ORDER_COLUMN_INDEX = {header: ORDER_HEADERS.index(header) for header in ORDER_HEADERS if header != "Order ID"}
ORDER_COLUMN_INDEX["Order ID"] = ORDER_PRIMARY_ID_INDEX

ANALYSIS_FIRST_VALUE_HEADERS = {"Order ID", "Order ID.1", "SKU ID"}

ANALYSIS_SOURCE_ALIASES = {
    "Affiliate Shop Ads commission\t": ["Affiliate Shop Ads commission\t", "Affiliate Shop Ads commission"],
}

TOP_LEVEL_OWNER_MAP = {
    "品牌美妆": "阳玲",
    "母婴": "阳玲",
    "汽配": "阳玲",
}


@dataclass(frozen=True)
class SourceFile:
    path: Path
    kind: str
    store_label: str


@dataclass(frozen=True)
class StoreAdjustment:
    store_label: str
    month: int
    order_id: str | None
    amount: float
    note: str
    source_file: str
    source_type: str


def normalize_header(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\t", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def clean_id(value: object) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def collect_order_ids(order_files: list[SourceFile], include_canceled: bool) -> set[str]:
    order_ids: set[str] = set()
    for source in order_files:
        ws, header_row = open_worksheet(source.path, required_headers=["Order ID", "Order Status"])
        source_index = {normalize_header(header): idx for idx, header in enumerate(header_row)}
        order_id_index = source_index.get(normalize_header("Order ID"))
        status_index = source_index.get(normalize_header("Order Status"))
        if order_id_index is None:
            continue

        for row in ws.iter_rows(min_row=3, values_only=True):
            order_id = clean_id(row[order_id_index] if order_id_index < len(row) else None)
            if not order_id:
                continue
            order_status = row[status_index] if status_index is not None and status_index < len(row) else None
            if not include_canceled and order_status in {"Canceled", "Cancelled"}:
                continue
            order_ids.add(order_id)
    return order_ids


def resolve_payout_order_key(row_values: dict[str, object], valid_order_ids: set[str]) -> str | None:
    related_order_id = clean_id(row_values.get("Related order ID"))
    if related_order_id and related_order_id in valid_order_ids:
        return related_order_id

    adjustment_id = clean_id(row_values.get("Order/adjustment ID"))
    if adjustment_id and adjustment_id in valid_order_ids:
        return adjustment_id

    return None


def to_number(value: object) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def safe_sum(values: Iterable[object]) -> float | None:
    total = 0.0
    has_value = False
    for value in values:
        number = to_number(value)
        if number is None:
            continue
        total += number
        has_value = True
    return total if has_value else None


def clean_cell_value(value: object) -> object:
    if isinstance(value, str):
        return ILLEGAL_CHARACTERS_RE.sub("", value)
    return value


def clean_row(values: Iterable[object]) -> list[object]:
    return [clean_cell_value(value) for value in values]


def first_non_empty(series: pd.Series) -> object:
    for value in series:
        if pd.notna(value) and value != "":
            return clean_cell_value(value)
    return None


def to_excel_value(value: object) -> object:
    if pd.isna(value):
        return None
    return clean_cell_value(value)


def resolve_analysis_source_column(df: pd.DataFrame, header: str) -> str | None:
    candidates = ANALYSIS_SOURCE_ALIASES.get(header, [header])
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def build_generated_dir(workspace: Path) -> Path:
    return workspace / "01-TK文件" / GENERATED_DIR_NAME


def autosize_worksheet(worksheet, min_width: int = 12, extra: int = 2) -> None:
    for column_cells in worksheet.columns:
        letter = column_cells[0].column_letter
        max_length = 0
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))
        worksheet.column_dimensions[letter].width = max(min_width, min(max_length + extra, 36))


def style_header_row(worksheet, fill: PatternFill, font: Font, alignment: Alignment, border: Border) -> None:
    for cell in worksheet[1]:
        cell.fill = fill
        cell.font = font
        cell.alignment = alignment
        cell.border = border


def apply_fill_to_headers(worksheet, headers: set[str], fill: PatternFill, font: Font | None = None) -> None:
    target_font = font or Font(bold=True)
    for cell in worksheet[1]:
        if cell.value in headers:
            cell.fill = fill
            cell.font = target_font


def is_truthy(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "是", "启用", "x"}


def set_order_value(row: list[object], header: str, value: object) -> None:
    if header == "Order ID":
        row[ORDER_PRIMARY_ID_INDEX] = value
        row[ORDER_SECONDARY_ID_INDEX] = value
        return
    index = ORDER_COLUMN_INDEX[header]
    row[index] = value


def get_order_value(row: list[object], header: str) -> object:
    if header == "Order ID":
        return row[ORDER_PRIMARY_ID_INDEX]
    return row[ORDER_COLUMN_INDEX[header]]


def append_note(existing: object, addition: object) -> str | None:
    left = "" if existing is None else str(existing).strip()
    right = "" if addition is None else str(addition).strip()
    if not left:
        return right or None
    if not right:
        return left
    if right in left:
        return left
    return f"{left}; {right}"


def distribute_amount(total_amount: float, weights: list[float]) -> list[float]:
    if not weights:
        return []
    positive_weights = [weight if weight > 0 else 0.0 for weight in weights]
    if sum(positive_weights) <= 0:
        positive_weights = [1.0] * len(weights)

    raw_values = [total_amount * weight / sum(positive_weights) for weight in positive_weights]
    rounded = [round(value, 2) for value in raw_values]
    drift = round(total_amount - sum(rounded), 2)
    if rounded:
        rounded[-1] = round(rounded[-1] + drift, 2)
    return rounded


def recompute_order_row(
    row: list[object],
    tax_rate: float,
    exchange_rate: float,
    overrides: dict[str, object] | None = None,
) -> list[object]:
    overrides = overrides or {}
    total_revenue = to_number(get_order_value(row, "Total revenue"))
    fee_values = [to_number(get_order_value(row, field)) for field in ORDER_EXTRA_FEE_FIELDS]
    ad_fee = to_number(get_order_value(row, "广告费"))

    total_fees = to_number(overrides.get("覆盖_Total fees"))
    if total_fees is None:
        total_fees = safe_sum([*fee_values, ad_fee])

    settled_amount = to_number(overrides.get("覆盖_结算金额"))
    if settled_amount is None:
        settled_amount = safe_sum([total_revenue, total_fees])

    tax_fee = to_number(overrides.get("覆盖_税费"))
    if tax_fee is None:
        tax_fee = settled_amount * tax_rate if settled_amount is not None else None

    final_settlement = to_number(overrides.get("覆盖_总计结算金额"))
    if final_settlement is None:
        final_settlement = settled_amount - tax_fee if settled_amount is not None and tax_fee is not None else None

    net_profit = to_number(overrides.get("覆盖_净利润(rmb)"))
    if net_profit is None:
        total_cost = to_number(get_order_value(row, "SKU总成本(rmb)"))
        net_profit = (final_settlement / exchange_rate - total_cost) if final_settlement is not None and total_cost is not None else None

    set_order_value(row, "Total fees", total_fees)
    set_order_value(row, "结算金额", settled_amount)
    set_order_value(row, "税费", tax_fee)
    set_order_value(row, "总计结算金额", final_settlement)
    set_order_value(row, "净利润(rmb)", net_profit)
    return row


def build_empty_adjustment_row(store_label: str, month: object, sequence: int) -> list[object]:
    row = [None] * len(ORDER_HEADERS)
    synthetic_id = f"ADJ-{month or 'NA'}-{sequence:04d}"
    set_order_value(row, "店铺", store_label)
    set_order_value(row, "Order ID", synthetic_id)
    set_order_value(row, "是否到款", "是")
    set_order_value(row, "Order Status", None)
    set_order_value(row, "Quantity", 0)
    set_order_value(row, "SKU成本(rmb)", 0)
    set_order_value(row, "SKU总成本(rmb)", 0)
    set_order_value(row, "广告费", 0)
    set_order_value(row, "二次销售(0/1)", 0)
    return row


def create_store_adjustment_row(
    adjustment: StoreAdjustment,
    sequence: int,
    tax_rate: float,
    exchange_rate: float,
) -> list[object]:
    row = build_empty_adjustment_row(adjustment.store_label, adjustment.month, sequence)
    if adjustment.order_id:
        set_order_value(row, "Order ID", adjustment.order_id)
    set_order_value(row, "是否到款", "是")
    set_order_value(row, "广告费", adjustment.amount)
    set_order_value(row, "备注", adjustment.note)
    return clean_row(recompute_order_row(row, tax_rate, exchange_rate))


def build_marketing_store_adjustments(marketing_files: list[SourceFile]) -> list[StoreAdjustment]:
    adjustments: list[StoreAdjustment] = []
    for source in marketing_files:
        workbook = load_workbook(source.path, read_only=True, data_only=True)
        month = extract_month(source.path.name) or 2
        for worksheet in workbook.worksheets:
            if worksheet.sheet_state != "visible":
                continue

            preview_rows = list(worksheet.iter_rows(min_row=1, max_row=3, values_only=True))
            if len(preview_rows) < 2:
                continue

            header_row = [str(value).strip() if value is not None else "" for value in preview_rows[1]]
            if "线下广告费金额（PHP）" not in header_row or "平台" not in header_row:
                continue

            source_index = {header: idx for idx, header in enumerate(header_row)}
            for row in worksheet.iter_rows(min_row=3, values_only=True):
                platform = str(row[source_index.get("平台", -1)] or "").strip()
                if platform in {"", "合计："}:
                    continue
                if platform.upper() != "TK":
                    continue

                owner = str(row[source_index.get("业务员", -1)] or "").strip()
                category = str(row[source_index.get("类目", -1)] or "").strip()
                store_label = f"{month}月份-{owner}{category}"

                offline_amount = to_number(row[source_index.get("线下广告费金额（PHP）", -1)]) or 0.0
                if offline_amount:
                    adjustments.append(
                        StoreAdjustment(
                            store_label=store_label,
                            month=month,
                            order_id=None,
                            amount=-offline_amount,
                            note="系统重建-线下广告费",
                            source_file=source.path.name,
                            source_type="offline_ad",
                        )
                    )
    return adjustments


def apply_system_adjustments(
    order_rows: list[list[object]],
    adjustments: list[StoreAdjustment],
    tax_rate: float,
    exchange_rate: float,
) -> tuple[list[list[object]], dict[str, int]]:
    stats = {"online_ad_rows": 0, "offline_ad_rows": 0}
    sequence = len(order_rows) + 1
    for adjustment in adjustments:
        order_rows.append(create_store_adjustment_row(adjustment, sequence, tax_rate, exchange_rate))
        sequence += 1
        if adjustment.source_type == "online_ad":
            stats["online_ad_rows"] += 1
        elif adjustment.source_type == "offline_ad":
            stats["offline_ad_rows"] += 1
    return order_rows, stats


def load_ad_import(ad_import_path: Path | None) -> pd.DataFrame:
    if ad_import_path is None or not ad_import_path.exists():
        return pd.DataFrame(columns=AD_IMPORT_HEADERS)
    df = pd.read_excel(ad_import_path, sheet_name=AD_IMPORT_SHEET_NAME)
    for header in AD_IMPORT_HEADERS:
        if header not in df.columns:
            df[header] = None
    df = df[AD_IMPORT_HEADERS].copy()
    return df[df["启用"].apply(is_truthy)].reset_index(drop=True)


def match_filter(row: list[object], rule: pd.Series) -> bool:
    if str(rule.get("店铺") or "").strip() and str(get_order_value(row, "店铺") or "").strip() != str(rule.get("店铺") or "").strip():
        return False

    paid_filter = str(rule.get("筛选_是否到款") or "").strip()
    if paid_filter and str(get_order_value(row, "是否到款") or "").strip() != paid_filter:
        return False

    status_filter = str(rule.get("筛选_Order Status") or "").strip()
    if status_filter and str(get_order_value(row, "Order Status") or "").strip() != status_filter:
        return False

    seller_sku_filter = clean_id(rule.get("筛选_Seller SKU"))
    if seller_sku_filter and clean_id(get_order_value(row, "Seller SKU")) != seller_sku_filter:
        return False

    start_time = pd.to_datetime(rule.get("筛选_开始时间"), errors="coerce")
    if pd.notna(start_time):
        created_time = pd.to_datetime(get_order_value(row, "Created Time"), errors="coerce")
        if pd.isna(created_time) or created_time < start_time:
            return False

    end_time = pd.to_datetime(rule.get("筛选_结束时间"), errors="coerce")
    if pd.notna(end_time):
        created_time = pd.to_datetime(get_order_value(row, "Created Time"), errors="coerce")
        if pd.isna(created_time) or created_time > end_time:
            return False

    return True


def allocation_weight(row: list[object], method: str) -> float:
    source_header = AD_ALLOCATION_METHODS.get(method)
    if source_header is None:
        return 1.0
    value = to_number(get_order_value(row, source_header))
    if value is None:
        return 0.0
    return max(value, 0.0)


def apply_ad_import(
    order_rows: list[list[object]],
    ad_import_df: pd.DataFrame,
    tax_rate: float,
    exchange_rate: float,
) -> tuple[list[list[object]], dict[str, int]]:
    if ad_import_df.empty:
        return order_rows, {"allocation_rules": 0, "adjustment_rows": 0}

    allocation_rule_count = 0
    adjustment_row_count = 0

    for _, rule in ad_import_df.iterrows():
        record_type = str(rule.get("记录类型") or "").strip()
        if record_type in AD_IMPORT_ALLOCATION_TYPES:
            method = str(rule.get("分摊方式") or "平均到订单").strip() or "平均到订单"
            if method not in AD_ALLOCATION_METHODS:
                raise ValueError(f"不支持的广告费分摊方式: {method}")

            selected_rows = [row for row in order_rows if match_filter(row, rule)]
            if not selected_rows:
                raise ValueError(f"广告费分摊规则未匹配到任何订单: 店铺={rule.get('店铺')} 来源单据={rule.get('来源单据')}")

            amount = to_number(rule.get("分摊金额(PHP)"))
            if amount is None:
                raise ValueError("广告费分摊规则缺少 分摊金额(PHP)")

            weights = [allocation_weight(row, method) for row in selected_rows]
            distributed = distribute_amount(amount, weights)
            allocation_note = append_note(rule.get("备注"), rule.get("来源单据"))

            for row, share in zip(selected_rows, distributed):
                current_ad_fee = to_number(get_order_value(row, "广告费")) or 0.0
                set_order_value(row, "广告费", round(current_ad_fee + share, 2))
                if allocation_note:
                    set_order_value(row, "备注", append_note(get_order_value(row, "备注"), allocation_note))
                recompute_order_row(row, tax_rate, exchange_rate)

            allocation_rule_count += 1
            continue

        if record_type in AD_IMPORT_ADJUSTMENT_TYPES:
            adjustment_row = build_empty_adjustment_row(rule.get("店铺"), rule.get("月份"), len(order_rows) + adjustment_row_count + 1)
            if str(rule.get("调整_Order ID") or "").strip():
                set_order_value(adjustment_row, "Order ID", str(rule.get("调整_Order ID")).strip())
            if str(rule.get("调整_是否到款") or "").strip():
                set_order_value(adjustment_row, "是否到款", str(rule.get("调整_是否到款")).strip())
            if pd.notna(rule.get("调整_Order Status")):
                set_order_value(adjustment_row, "Order Status", rule.get("调整_Order Status"))
            if pd.notna(rule.get("调整_Seller SKU")):
                set_order_value(adjustment_row, "Seller SKU", rule.get("调整_Seller SKU"))
            if pd.notna(rule.get("调整_Quantity")):
                set_order_value(adjustment_row, "Quantity", to_number(rule.get("调整_Quantity")) or 0)

            for header in ["Total revenue", *ORDER_EXTRA_FEE_FIELDS, "广告费", "备注", "退款原因", "二次销售(0/1)"]:
                if pd.notna(rule.get(header)) and rule.get(header) != "":
                    set_order_value(adjustment_row, header, rule.get(header))

            adjustment_row = recompute_order_row(
                adjustment_row,
                tax_rate,
                exchange_rate,
                overrides={
                    "覆盖_Total fees": rule.get("覆盖_Total fees"),
                    "覆盖_结算金额": rule.get("覆盖_结算金额"),
                    "覆盖_税费": rule.get("覆盖_税费"),
                    "覆盖_总计结算金额": rule.get("覆盖_总计结算金额"),
                    "覆盖_净利润(rmb)": rule.get("覆盖_净利润(rmb)"),
                },
            )
            order_rows.append(clean_row(adjustment_row))
            adjustment_row_count += 1
            continue

        raise ValueError(f"不支持的记录类型: {record_type}")

    return order_rows, {
        "allocation_rules": allocation_rule_count,
        "adjustment_rows": adjustment_row_count,
    }


def export_ad_import_template(output_path: Path) -> None:
    workbook = Workbook()
    data_sheet = workbook.active
    data_sheet.title = AD_IMPORT_SHEET_NAME
    data_sheet.freeze_panes = "A2"
    data_sheet.append(AD_IMPORT_HEADERS)
    data_sheet.append([
        0,
        "分摊广告费",
        2,
        "2月份-刘林长3C",
        "刘林长 菲律宾 2月营销费+退款统计.xlsx",
        3,
        -3549.42,
        "按Total revenue",
        "是",
        "Completed",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "2月广告费自动分摊",
        None,
        None,
    ])
    data_sheet.append([
        0,
        "新增调整行",
        2,
        "2月份-刘林长3C",
        "人工补录",
        1,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        "ADJ-SAMPLE-0001",
        "是",
        None,
        None,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        None,
        None,
        -127.71,
        None,
        None,
        "Withholding tax",
        None,
        0,
    ])

    header_border = Border(
        left=Side(style="thin", color="00B7B7B7"),
        right=Side(style="thin", color="00B7B7B7"),
        top=Side(style="thin", color="00B7B7B7"),
        bottom=Side(style="thin", color="00B7B7B7"),
    )
    base_header_fill = PatternFill(fill_type="solid", fgColor="00D9E2F3")
    always_required_fill = PatternFill(fill_type="solid", fgColor="00FFD966")
    allocation_required_fill = PatternFill(fill_type="solid", fgColor="00F4B183")
    allocation_optional_fill = PatternFill(fill_type="solid", fgColor="00FFF2CC")
    adjustment_optional_fill = PatternFill(fill_type="solid", fgColor="00DDEBF7")
    override_fill = PatternFill(fill_type="solid", fgColor="00E2F0D9")
    common_font = Font(bold=True, color="001F1F1F")
    center_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    style_header_row(data_sheet, base_header_fill, common_font, center_alignment, header_border)
    apply_fill_to_headers(data_sheet, AD_IMPORT_ALWAYS_REQUIRED_HEADERS, always_required_fill, common_font)
    apply_fill_to_headers(data_sheet, AD_IMPORT_ALLOCATION_REQUIRED_HEADERS, allocation_required_fill, common_font)
    apply_fill_to_headers(data_sheet, AD_IMPORT_ALLOCATION_OPTIONAL_HEADERS, allocation_optional_fill, common_font)
    apply_fill_to_headers(
        data_sheet,
        {header for header in AD_IMPORT_ADJUSTMENT_OPTIONAL_HEADERS if not header.startswith("覆盖_")},
        adjustment_optional_fill,
        common_font,
    )
    apply_fill_to_headers(data_sheet, {header for header in AD_IMPORT_ADJUSTMENT_OPTIONAL_HEADERS if header.startswith("覆盖_")}, override_fill, common_font)

    number_headers = {
        "分摊金额(PHP)",
        "调整_Quantity",
        "Total revenue",
        "Transaction fee",
        "TikTok Shop commission fee",
        "Seller shipping fee",
        "Affiliate commission",
        "Affiliate Shop Ads commission",
        "Order processing fee",
        "Bonus cashback service fee",
        "Affiliate commission deposit",
        "Shipping Service Fee",
        "Voucher Xtra Service Fee",
        "Pre-order service fee",
        "Affiliate partner commission",
        "LIVE Specials service fee",
        "Campaign resource fee",
        "EAMS Program service fee",
        "GMV Max Coupon",
        "广告费",
        "覆盖_Total fees",
        "覆盖_结算金额",
        "覆盖_税费",
        "覆盖_总计结算金额",
        "覆盖_净利润(rmb)",
        "二次销售(0/1)",
    }
    date_headers = {"筛选_开始时间", "筛选_结束时间"}
    for row in data_sheet.iter_rows(min_row=2, max_row=data_sheet.max_row, max_col=data_sheet.max_column):
        for cell in row:
            cell.border = header_border
            cell.alignment = Alignment(vertical="center")
            header = data_sheet.cell(row=1, column=cell.column).value
            if header in number_headers:
                cell.number_format = "#,##0.00"
            elif header in date_headers:
                cell.number_format = "yyyy-mm-dd"

    data_sheet.row_dimensions[1].height = 36
    autosize_worksheet(data_sheet)

    guide_sheet = workbook.create_sheet(AD_IMPORT_GUIDE_SHEET_NAME)
    guide_sheet.freeze_panes = "A2"
    guide_rows = [
        ["字段", "说明"],
        ["颜色图例", "黄色=始终必填；橙色=分摊广告费必填；浅黄=分摊广告费可选筛选；浅蓝=新增调整行可填；浅绿=新增调整行覆盖值"],
        ["记录类型", "只允许：分摊广告费、新增调整行"],
        ["系统默认广告费逻辑", "线上广告费默认来自到款明细表 Type=GMV Payment for TikTok Ads；线下广告费默认来自营销表可见页的线下广告费金额（PHP）；二者都按店铺级调整行写入，不拆分到订单"],
        ["分摊广告费", "只用于系统未自动识别到的额外广告费场景；按店铺和筛选条件匹配真实订单，再把 分摊金额(PHP) 分摊到 广告费 列"],
        ["新增调整行", "用于补原利润表里状态为空的人工调整行；可直接填写财务列，并允许覆盖 Total fees/结算金额/税费/总计结算金额/净利润"],
        ["分摊方式", "只允许：按订单金额、按Total revenue、按结算金额、按数量、平均到订单"],
        ["金额符号", "费用填负数，补贴/返还填正数，必须与利润表最终方向一致"],
        ["店铺", "必须与程序生成表里的 店铺 完全一致，例如 2月份-刘林长3C"],
        ["覆盖列", "只有新增调整行才需要；留空则脚本按系统公式自动重算"],
        ["最小人工原则", "优先使用系统自动重建结果；只有无法从系统规则推导的补贴/税费/特殊广告费，再填写导入表"],
    ]
    for row in guide_rows:
        guide_sheet.append(row)

    guide_header_fill = PatternFill(fill_type="solid", fgColor="00333F4F")
    guide_header_font = Font(bold=True, color="00FFFFFF")
    style_header_row(guide_sheet, guide_header_fill, guide_header_font, center_alignment, header_border)
    for row in guide_sheet.iter_rows(min_row=2, max_row=guide_sheet.max_row, max_col=guide_sheet.max_column):
        for cell in row:
            cell.border = header_border
            cell.alignment = Alignment(vertical="top", wrap_text=True)
    autosize_worksheet(guide_sheet, min_width=16, extra=4)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(output_path)
    print(f"已导出广告费导入模板: {output_path}")


def build_analysis_dataframe(order_workbook: Path, exchange_rate: float) -> pd.DataFrame:
    df = pd.read_excel(order_workbook, sheet_name="Tiktok订单完成表")
    group_keys = ["店铺", "是否到款"]
    prepared = df[group_keys].copy()
    agg_map: dict[str, str | callable] = {}

    for header in ANALYSIS_HEADERS[2:]:
        if header in {"总收入(RMB)", "利润率"}:
            continue

        source_column = resolve_analysis_source_column(df, header)
        if header in ANALYSIS_FIRST_VALUE_HEADERS:
            prepared[header] = df[source_column] if source_column else None
            agg_map[header] = first_non_empty
        else:
            prepared[header] = pd.to_numeric(df[source_column], errors="coerce").fillna(0) if source_column else 0.0
            agg_map[header] = "sum"

    grouped = prepared.groupby(group_keys, dropna=False, sort=True).agg(agg_map).reset_index()
    grouped["总收入(RMB)"] = grouped["Total revenue"] / exchange_rate
    grouped["利润率"] = grouped.apply(
        lambda row: row["净利润(rmb)"] / row["总收入(RMB)"] if row["总收入(RMB)"] not in (0, None) else 0,
        axis=1,
    )
    return grouped[ANALYSIS_HEADERS]


def write_analysis_sheet(output_path: Path, analysis_df: pd.DataFrame) -> None:
    workbook = Workbook()
    header_fill = PatternFill(fill_type="solid", fgColor="004F81BD")
    header_font = Font(bold=True, color="00FFFFFF")
    header_alignment = Alignment(horizontal="center", vertical="center")
    data_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    worksheet = workbook.active
    worksheet.title = "透视"
    worksheet.freeze_panes = "C2"
    worksheet.append(ANALYSIS_HEADERS)

    numeric_columns = set(range(3, 50))
    text_columns = {50, 51, 52}
    for row_values in analysis_df.itertuples(index=False, name=None):
        worksheet.append([to_excel_value(value) for value in row_values])

    for cell in worksheet[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = header_alignment

    for column_index in range(1, len(ANALYSIS_HEADERS) + 1):
        worksheet.column_dimensions[get_column_letter(column_index)].width = 20

    for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row, max_col=worksheet.max_column):
        for cell in row:
            cell.border = data_border
            if cell.column == 48:
                cell.number_format = "0.00%"
            elif cell.column in numeric_columns:
                cell.number_format = "#,##0.00"
            elif cell.column in text_columns:
                cell.number_format = "@"

    temp_path = output_path.with_name(f"{output_path.stem}__analysis_temp.xlsx")
    workbook.save(temp_path)
    try:
        insert_sheet_via_excel_com(output_path, temp_path, "透视")
    finally:
        if temp_path.exists():
            temp_path.unlink()


def insert_sheet_via_excel_com(target_workbook: Path, sheet_workbook: Path, sheet_name: str) -> None:
    target = str(target_workbook.resolve()).replace("'", "''")
    source = str(sheet_workbook.resolve()).replace("'", "''")
    name = sheet_name.replace("'", "''")
    script = f"""
$ErrorActionPreference = 'Stop'
$excel = $null
$targetWb = $null
$sourceWb = $null
try {{
    $excel = New-Object -ComObject Excel.Application
    $excel.Visible = $false
    $excel.DisplayAlerts = $false
    $targetWb = $excel.Workbooks.Open('{target}')
    $sourceWb = $excel.Workbooks.Open('{source}')
    foreach ($sheet in @($targetWb.Worksheets)) {{
        if ($sheet.Name -eq '{name}') {{
            $sheet.Delete()
            break
        }}
    }}
    $sourceWb.Worksheets.Item('{name}').Copy($targetWb.Worksheets.Item(1))
    $targetWb.Worksheets.Item(1).Name = '{name}'
    $targetWb.Save()
}}
finally {{
    if ($sourceWb -ne $null) {{ try {{ $sourceWb.Close($false) }} catch {{}} }}
    if ($targetWb -ne $null) {{ try {{ $targetWb.Close($true) }} catch {{}} }}
    if ($excel -ne $null) {{
        try {{ $excel.Quit() }} catch {{}}
        try {{ [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) }} catch {{}}
    }}
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
}}
"""
    subprocess.run(["powershell", "-NoProfile", "-Command", script], check=True)


def add_analysis_sheet_only(output_path: Path, exchange_rate: float) -> None:
    analysis_df = build_analysis_dataframe(output_path, exchange_rate)
    write_analysis_sheet(output_path, analysis_df)
    print(f"已更新静态“透视”工作表: {output_path}")


def load_cost_map(cost_workbook: Path) -> dict[str, float]:
    wb = load_workbook(cost_workbook, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]
    costs: dict[str, float] = {}
    for sku, total_cost in ws.iter_rows(min_row=2, values_only=True):
        key = clean_id(sku)
        number = to_number(total_cost)
        if key and number is not None:
            costs[key] = number
    return costs


def extract_month(text: str) -> int | None:
    match = re.search(r"(\d{1,2})月", text)
    if not match:
        return None
    return int(match.group(1))


def extract_category(text: str) -> str:
    category = text
    category = category.replace("菲律宾 TK ", "")
    category = category.replace("菲律宾TK ", "")
    category = category.replace("菲律宾 ", "")
    category = re.sub(r"\d{4}年\d{1,2}月(?:到\d{4}年\d{1,2}月\d{1,2}日|到\d{1,2}月\d{1,2}日)?", "", category)
    category = category.replace("店铺数据", "")
    category = category.replace("店铺", "")
    category = category.replace("订单", "")
    category = category.replace("回款", "")
    category = category.replace("数据", "")
    category = category.replace(" ", "")
    return category.strip("-_")


def build_store_label(path: Path, system_dir: Path) -> str:
    relative_parts = path.relative_to(system_dir).parts
    parent_parts = relative_parts[:-1]
    month = extract_month(path.name) or next((extract_month(part) for part in reversed(parent_parts) if extract_month(part)), None) or 2

    if parent_parts and "-菲律宾TK" in parent_parts[0]:
        owner = parent_parts[0].split("-", 1)[0].strip()
        category_source = parent_parts[1] if len(parent_parts) > 1 else path.stem
    else:
        category_source = parent_parts[0] if parent_parts else path.stem
        category_guess = extract_category(category_source)
        owner = TOP_LEVEL_OWNER_MAP.get(category_guess)
        if owner is None:
            raise ValueError(f"无法从路径推断负责人: {path}")

    category = extract_category(category_source)
    return f"{month}月份-{owner}{category}"


def discover_source_files(system_dir: Path) -> list[SourceFile]:
    files: list[SourceFile] = []
    for path in system_dir.rglob("*.xlsx"):
        if path.name.startswith("~$"):
            continue
        name = path.name
        if "订单" in name:
            kind = "order"
        elif "回款" in name:
            kind = "payout"
        elif "营销费+退款统计" in name:
            kind = "marketing"
        else:
            continue
        files.append(SourceFile(path=path, kind=kind, store_label=build_store_label(path, system_dir)))
    return sorted(files, key=lambda item: (item.kind, item.store_label, str(item.path)))


def open_worksheet(
    path: Path,
    sheet_name: str | None = None,
    required_headers: Iterable[str] = (),
):
    workbook = load_workbook(path, read_only=True, data_only=True)
    worksheet = workbook[sheet_name] if sheet_name and sheet_name in workbook.sheetnames else workbook[workbook.sheetnames[0]]
    worksheet.reset_dimensions()
    header_row = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))
    header_keys = {normalize_header(value) for value in header_row if value is not None}
    required_keys = {normalize_header(value) for value in required_headers}

    if required_keys and not required_keys.issubset(header_keys):
        workbook = load_workbook(path, read_only=False, data_only=True)
        worksheet = workbook[sheet_name] if sheet_name and sheet_name in workbook.sheetnames else workbook[workbook.sheetnames[0]]
        header_row = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))

    return worksheet, header_row


def build_payout_summary(
    payout_files: list[SourceFile],
    payout_sheet,
    valid_order_ids: set[str],
) -> tuple[dict[str, dict[str, float | str | None]], int, list[StoreAdjustment]]:
    payout_sheet.append(clean_row(PAYOUT_HEADERS))
    payout_index = {normalize_header(header): idx for idx, header in enumerate(PAYOUT_HEADERS[1:])}
    aggregates: dict[str, dict[str, float | str | None]] = defaultdict(dict)
    adjustments: list[StoreAdjustment] = []
    row_count = 0

    for source in payout_files:
        ws, header_row = open_worksheet(
            source.path,
            "Order details",
            required_headers=[
                "Order/adjustment ID",
                "Type",
                "Affiliate commission deposit",
                "Campaign resource fee",
                "Order processing fee",
                "Related order ID",
            ],
        )
        source_index = {normalize_header(header): idx for idx, header in enumerate(header_row)}

        for row in ws.iter_rows(min_row=2, values_only=True):
            combined_row = [source.store_label] + [None] * (len(PAYOUT_HEADERS) - 1)
            row_values: dict[str, object] = {}

            for header in PAYOUT_HEADERS[1:]:
                normalized = normalize_header(header)
                source_idx = source_index.get(normalized)
                value = row[source_idx] if source_idx is not None and source_idx < len(row) else None
                combined_row[payout_index[normalized] + 1] = value
                row_values[header] = value

            payout_sheet.append(clean_row(combined_row))
            row_count += 1

            order_key = resolve_payout_order_key(row_values, valid_order_ids)
            if not order_key:
                payout_type = str(row_values.get("Type") or "").strip()
                total_settlement_amount = to_number(row_values.get("Total settlement amount"))
                adjustment_id = clean_id(row_values.get("Order/adjustment ID"))
                if payout_type == "GMV Payment for TikTok Ads" and adjustment_id and total_settlement_amount not in (None, 0):
                    adjustments.append(
                        StoreAdjustment(
                            store_label=source.store_label,
                            month=extract_month(source.path.name) or 2,
                            order_id=adjustment_id,
                            amount=float(total_settlement_amount),
                            note="系统重建-线上广告费",
                            source_file=source.path.name,
                            source_type="online_ad",
                        )
                    )
                continue

            bucket = aggregates[order_key]
            bucket["店铺"] = source.store_label
            for order_field, payout_field in PAYOUT_FIELD_ALIASES.items():
                number = to_number(row_values.get(payout_field))
                if number is None:
                    continue
                bucket[order_field] = to_number(bucket.get(order_field)) or 0.0
                bucket[order_field] = float(bucket[order_field]) + number

    return aggregates, row_count, adjustments


def build_order_summary(
    order_files: list[SourceFile],
    payout_aggregates: dict[str, dict[str, float | str | None]],
    cost_map: dict[str, float],
    tax_rate: float,
    exchange_rate: float,
    include_canceled: bool,
) -> list[list[object]]:
    order_rows: list[list[object]] = []

    for source in order_files:
        ws, header_row = open_worksheet(source.path, required_headers=["Order ID", "Seller SKU", "Quantity"])
        source_index = {normalize_header(header): idx for idx, header in enumerate(header_row)}

        for row in ws.iter_rows(min_row=3, values_only=True):
            order_id = clean_id(row[source_index[normalize_header("Order ID")]])
            if not order_id:
                continue

            order_status = row[source_index[normalize_header("Order Status")]]
            if not include_canceled and order_status in {"Canceled", "Cancelled"}:
                continue

            seller_sku = clean_id(row[source_index[normalize_header("Seller SKU")]])
            quantity = to_number(row[source_index[normalize_header("Quantity")]]) or 0.0
            payout = payout_aggregates.get(order_id, {})

            total_revenue = payout.get("Total revenue")
            if total_revenue is None:
                total_revenue = safe_sum([
                    row[source_index[normalize_header("SKU Subtotal After Discount")]],
                    -1 * (to_number(row[source_index[normalize_header("Shipping Fee Seller Discount")]]) or 0.0),
                    -1 * (to_number(row[source_index[normalize_header("Taxes")]]) or 0.0),
                ])

            sku_cost = cost_map.get(seller_sku) if seller_sku else None
            total_cost = sku_cost * quantity if sku_cost is not None else None

            fee_values = [payout.get(field) for field in ORDER_EXTRA_FEE_FIELDS]
            ad_fee = None
            total_fees = safe_sum([*fee_values, ad_fee])
            settled_amount = safe_sum([total_revenue, total_fees])
            tax_fee = settled_amount * tax_rate if settled_amount is not None else None
            final_settlement = settled_amount - tax_fee if settled_amount is not None else None
            net_profit = (final_settlement / exchange_rate - total_cost) if final_settlement is not None and total_cost is not None else None

            source_values = list(row)
            combined_row = [
                source.store_label,
                order_id,
                order_id,
                "是" if order_id in payout_aggregates else "否",
                *source_values[1:55],
                total_revenue,
                sku_cost,
                total_cost,
                *fee_values,
                ad_fee,
                total_fees,
                settled_amount,
                tax_fee,
                final_settlement,
                net_profit,
                source_values[57] if len(source_values) > 57 else None,
                None,
                source_values[55] if len(source_values) > 55 else None,
                source_values[56] if len(source_values) > 56 else None,
            ]
            order_rows.append(clean_row(combined_row))

    return order_rows


def build_workbook(
    system_dir: Path,
    cost_workbook: Path,
    ad_import_path: Path | None,
    output_path: Path,
    tax_rate: float,
    exchange_rate: float,
    include_canceled: bool,
    add_analysis_sheet: bool,
) -> None:
    sources = discover_source_files(system_dir)
    order_files = [item for item in sources if item.kind == "order"]
    payout_files = [item for item in sources if item.kind == "payout"]
    marketing_files = [item for item in sources if item.kind == "marketing"]

    if not order_files or not payout_files:
        raise ValueError("未找到完整的订单表和回款表。")

    cost_map = load_cost_map(cost_workbook)
    valid_order_ids = collect_order_ids(order_files, include_canceled)

    workbook = Workbook(write_only=True)
    order_sheet = workbook.create_sheet("Tiktok订单完成表")
    payout_sheet = workbook.create_sheet("Tiktok到款明细表")

    payout_aggregates, payout_rows, payout_adjustments = build_payout_summary(payout_files, payout_sheet, valid_order_ids)
    order_rows = build_order_summary(
        order_files,
        payout_aggregates,
        cost_map,
        tax_rate,
        exchange_rate,
        include_canceled,
    )
    marketing_adjustments = build_marketing_store_adjustments(marketing_files)
    order_rows, system_adjustment_stats = apply_system_adjustments(
        order_rows,
        [*payout_adjustments, *marketing_adjustments],
        tax_rate,
        exchange_rate,
    )
    ad_import_df = load_ad_import(ad_import_path)
    order_rows, ad_import_stats = apply_ad_import(order_rows, ad_import_df, tax_rate, exchange_rate)

    order_sheet.append(clean_row(ORDER_HEADERS))
    for row in order_rows:
        order_sheet.append(row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(output_path)

    if add_analysis_sheet:
        add_analysis_sheet_only(output_path, exchange_rate)

    print(f"已生成: {output_path}")
    print(f"订单文件: {len(order_files)} 个, 汇总行数: {len(order_rows)}")
    print(f"回款文件: {len(payout_files)} 个, 汇总行数: {payout_rows}")
    print(f"营销统计文件: {len(marketing_files)} 个")
    print(
        "系统广告费调整: "
        f"线上广告费行 {system_adjustment_stats['online_ad_rows']} 条, "
        f"线下广告费行 {system_adjustment_stats['offline_ad_rows']} 条"
    )
    print(
        "广告费导入规则: "
        f"分摊 {ad_import_stats['allocation_rules']} 条, "
        f"新增调整行 {ad_import_stats['adjustment_rows']} 条"
    )
    if add_analysis_sheet:
        print("已生成静态“透视”工作表，样式对齐到盈利分析详细精美版模板。")
    print("说明: 当前脚本已自动处理订单、回款、SKU 成本与可选广告费导入表。默认跳过取消订单。")


def parse_args() -> argparse.Namespace:
    workspace = Path(__file__).resolve().parents[1]
    generated_dir = build_generated_dir(workspace)
    parser = argparse.ArgumentParser(description="整合 TikTok 订单表和回款表，生成汇总工作簿。")
    parser.add_argument(
        "--system-dir",
        type=Path,
        default=workspace / "01-TK文件" / "tk" / "011-系统数据",
        help="011-系统数据 文件夹路径",
    )
    parser.add_argument(
        "--cost-workbook",
        type=Path,
        default=workspace / "成本表.xlsx",
        help="SKU 成本表路径",
    )
    parser.add_argument(
        "--ad-import",
        type=Path,
        default=generated_dir / "广告费导入表.xlsx",
        help="广告费导入表路径；不存在时按无补充表处理",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=generated_dir / "2026年菲律宾2月tiktok订单利润表_程序生成.xlsx",
        help="输出工作簿路径，默认写入 01-TK文件/程序生成结果",
    )
    parser.add_argument("--tax-rate", type=float, default=0.011, help="税率，默认 1.1%%")
    parser.add_argument("--exchange-rate", type=float, default=8.4672, help="PHP 对 RMB 汇率")
    parser.add_argument("--include-canceled", action="store_true", help="保留取消订单")
    parser.add_argument("--skip-analysis-sheet", action="store_true", help="不生成静态透视工作表")
    parser.add_argument("--analysis-only", action="store_true", help="仅基于现有输出文件生成静态透视工作表")
    parser.add_argument("--export-ad-template", action="store_true", help="导出广告费导入表模板后退出")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.export_ad_template:
        export_ad_import_template(args.ad_import)
        return

    if args.analysis_only:
        add_analysis_sheet_only(args.output, args.exchange_rate)
        return

    build_workbook(
        system_dir=args.system_dir,
        cost_workbook=args.cost_workbook,
        ad_import_path=args.ad_import,
        output_path=args.output,
        tax_rate=args.tax_rate,
        exchange_rate=args.exchange_rate,
        include_canceled=args.include_canceled,
        add_analysis_sheet=not args.skip_analysis_sheet,
    )


if __name__ == "__main__":
    main()