"""
config/column_mapping_open_orders.py

Raw legacy ERP column codes -> readable names, specific to the "Open Orders"
sheet. This is intentionally separate from column_mapping.py (order
history) because several raw codes here don't exist in that mapping,
and a couple of shared-looking codes (orpswt, orpamt) mean the same
thing but orsdat here is a redundant, fragile-format duplicate of
orsdt8 rather than a real distinct field.
"""

COLUMN_MAPPING_OPEN_ORDERS = {
    "orcomp": "company_code",
    "orsocs": "source_customer_code",
    "orshcs": "ship_to_customer_code",
    "orshnm": "customer_name",
    "orpo": "purchase_order_number",
    "ornum": "order_number",
    "orodt8": "order_date",
    "orsdt8": "scheduled_ship_date",
    "orstat": "order_status",
    "orman": "first_half_sku_code",
    "orprod": "unique_sku_code",
    "ordes1": "product_description",
    "orroq": "final_order_quantity",
    "orpsqt": "shipped_order_quantity",
    "ortype": "order_type",
    "orpewt": "estimated_order_weight",
    "rtname": "delivery_route_name",
    "rtnumb": "delivery_route",
    "orrqdt": "customer_requested_date",
    "orrvdt": "revised_delivery_date",
    "orpswt": "shipped_order_weight",
    "00011": "full_sku_code",
    "orpamt": "order_amount",
    "batch_id": "batch_id",
    "ingested_at": "ingested_at_datetime",
}

# Columns intentionally excluded from the pipeline entirely (not renamed,
# not carried forward) — documented here so it's a deliberate decision,
# not something silently missed:
#   orsdat  - redundant duplicate of orsdt8 (scheduled_ship_date), stored
#             in a fragile M-D-YY digit-concatenated format with no
#             leading zeros (e.g. "41526" = Apr 15 2026). Confirmed
#             identical to orsdt8 across sample rows.
#   orpval  - confirmed not needed.
#   oruser  - confirmed not relevant for open orders (unlike order
#             history, where the equivalent field is kept).
DROP_COLUMNS_OPEN_ORDERS = ["orsdat", "orpval", "oruser"]