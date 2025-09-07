from datetime import datetime
import frappe
from frappe import _
from frappe.utils import now
from frappe.utils import today
from frappe.utils.background_jobs import enqueue

LOGGER = frappe.logger("warehouse_jobs")

def main():
    run_tag = now() 
    try:

        LOGGER.info("Starting Warehouse processing at %s", now())

        warehouses = frappe.get_all(
            "Warehouse",
            filters={},
            ignore_permissions=True
        )

        LOGGER.info(warehouses)

        for wh in warehouses:
            enqueue(
                process_single_warehouse,
                queue="long",
                job_name=f"process-warehouse-{wh['name']}-{run_tag}",
                warehouse_name=wh["name"]
            )

        LOGGER.info("Queued %s warehouse jobs.", len(warehouses))

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("Warehouse processing failed"))
        LOGGER.error("Error in process_warehouse_values: %s", e)


def process_single_warehouse(warehouse_name: str):
    try:
        wh = frappe.get_doc("Warehouse", warehouse_name)
        now = datetime.now()
        day = now.day
        custom_renewal_day = int(wh.custom_renewal_day)
        custom_financial_limit = int(wh.custom_financial_limit)

        if day >= custom_renewal_day:
            wh.db_set("custom_financial_balance", custom_financial_limit)
            
        frappe.db.commit()

        LOGGER.info("Inside process_single_warehouse %s", wh.name)
        LOGGER.info("date %s", now())

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("process_single_warehouse failed"))
