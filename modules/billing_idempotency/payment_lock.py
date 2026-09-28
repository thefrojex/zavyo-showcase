"""
Race-safe, idempotent payment-benefit application for a Razorpay-backed
billing system.

Adapted/excerpted from a production codebase for portfolio purposes. Not
runnable standalone — `payments_collection` (Mongo handle) is defined
elsewhere in the original codebase. Deliberately stripped of the surrounding
route handlers' business logic (specific plan names, pricing, tax handling)
since that's not the point of this excerpt — this file isolates *just* the
concurrency-safety pattern.

## The problem

A single successful payment can trigger benefit application (granting
credits/activating a plan) from more than one code path, potentially at
the same time:
- Razorpay's server-to-server webhook fires.
- The client's browser also calls a "verify payment" endpoint after
  checkout completes.
- A reconciliation job re-checks pending payments periodically.

If any two of these run concurrently for the same payment, a naive
"read benefits_applied, if false apply benefits, then set it true" sequence
has a race window: both reads can see `False` before either write lands,
and the customer gets double-credited.

## The fix

`_claim_payment_benefits_lock` performs the check-and-claim as a single
atomic MongoDB `update_one` call. The filter requires the payment to not
already be marked applied, AND to not currently be mid-application by
another caller (or for that in-progress claim to be stale, i.e. abandoned
by a crashed process). The `$set` that marks the claim happens in the same
atomic operation as the filter check. Success is judged strictly by
`modified_count == 1` — never by a separate follow-up read, which would
reopen the same race it's meant to close.

The 10-minute staleness window exists so that a process that crashes after
claiming the lock but before completing/failing it doesn't permanently
block that payment from ever being processed.
"""

from datetime import datetime, timedelta

from bson import ObjectId


def _claim_payment_benefits_lock(payment_doc_id: ObjectId) -> bool:
    """
    Attempt to atomically claim the right to apply this payment's benefits.

    Returns True if this caller won the claim (i.e. is now responsible for
    calling `_complete_payment_benefits` or `_fail_payment_benefits`).
    Returns False if benefits are already applied, or another caller
    currently holds a non-stale claim — in which case the caller should
    treat this as an idempotent no-op, not an error.
    """
    stale_before = datetime.utcnow() - timedelta(minutes=10)
    result = payments_collection.update_one(  # noqa: F821 - Mongo handle defined elsewhere
        {
            "_id": payment_doc_id,
            "benefits_applied": {"$ne": True},
            "$or": [
                {"benefits_applying": {"$ne": True}},
                {"benefits_apply_started_at": {"$lt": stale_before}},
            ],
        },
        {
            "$set": {
                "benefits_applying": True,
                "benefits_apply_started_at": datetime.utcnow(),
            }
        },
    )
    return result.modified_count == 1


def _complete_payment_benefits(payment_doc_id: ObjectId) -> None:
    """Mark benefits as successfully applied and release the claim."""
    payments_collection.update_one(  # noqa: F821
        {"_id": payment_doc_id},
        {
            "$set": {
                "benefits_applied": True,
                "benefits_applied_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            },
            "$unset": {
                "benefits_applying": "",
                "benefits_apply_started_at": "",
                "benefits_apply_error": "",
            },
        },
    )


def _fail_payment_benefits(payment_doc_id: ObjectId, error_message: str) -> None:
    """
    Release the claim without marking success, so a later retry (or the
    staleness window) can pick this payment back up rather than leaving it
    permanently locked by a failed attempt.
    """
    payments_collection.update_one(  # noqa: F821
        {"_id": payment_doc_id},
        {
            "$set": {
                "benefits_apply_error": error_message,
                "updated_at": datetime.utcnow(),
            },
            "$unset": {
                "benefits_applying": "",
                "benefits_apply_started_at": "",
            },
        },
    )


# --- Example caller shape (illustrative, not the original route handler) ---
#
# def verify_payment(payment_doc, razorpay_payment_id, razorpay_signature):
#     if not _verify_signature(payment_doc, razorpay_payment_id, razorpay_signature):
#         raise ValueError("Invalid payment signature")
#
#     if payment_doc.get("benefits_applied"):
#         return {"status": "success", "idempotent": True}  # already done, no-op
#
#     if not _claim_payment_benefits_lock(payment_doc["_id"]):
#         # Someone else is applying it right now (or just did) — don't apply twice.
#         return {"status": "success", "idempotent": True}
#
#     try:
#         _apply_plan_or_credits(payment_doc)  # the actual business logic
#         _complete_payment_benefits(payment_doc["_id"])
#         return {"status": "success", "idempotent": False}
#     except Exception as error:
#         _fail_payment_benefits(payment_doc["_id"], str(error))
#         raise
