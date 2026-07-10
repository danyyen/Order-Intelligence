"""
config/column_mapping_inventory.py

Column mapping for the warehouse inventory snapshot Excel export.

Unlike order_history/open_orders (AS400 exports with raw coded column
names), this source already has readable column headers — the mapping
here is mostly about normalizing naming convention, not decoding codes.

SKU here is NOT the same format as full_sku_code elsewhere in the
pipeline — it's stored as a plain number with leading zeros stripped
(an Excel gotcha). pseudonymize_inventory.py reconstructs full_sku_code
from it; this mapping just renames the raw column, it doesn't transform
the value.
"""

COLUMN_MAPPING_INVENTORY = {
    "sku": "raw_sku",
    "description": "product_description",
    "pallet_no.": "pallet_number",
    "lot_no.": "lot_number",
    "slot": "slot_location",
    "pallet_held": "pallet_held_flag",
    "prod'n_date": "production_date",
    "bb_date": "best_before_date",
    "res_qty": "reserved_quantity",
    "qty": "quantity_on_hand",
    "wgt": "weight",
}

# No columns are dropped from this source — every field is kept.
DROP_COLUMNS_INVENTORY = []