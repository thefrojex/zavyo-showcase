"""
Multi-tenant org context resolution and permission enforcement.

Adapted/excerpted from a production FastAPI backend for a multi-tenant SaaS
platform, for portfolio purposes. Not runnable standalone — helpers such as
`get_effective_access_context`, `has_permission`, `evaluate_org_subscription_access`
and the Mongo collection handles are defined elsewhere in the original codebase
and intentionally omitted here to keep this excerpt focused on the isolation
pattern itself, not the full RBAC/billing implementation.

The pattern this demonstrates: every authenticated request resolves a single
`context` object (org, effective permissions, subscription state) exactly once,
and every downstream check (`ensure_permission`, `ensure_write_access`) reads
from that same object rather than re-deriving tenant scope independently.
"""

from typing import Callable, Optional

from bson import ObjectId
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),  # noqa: F821 - security scheme defined elsewhere
):
    """Resolve and validate the JWT, then load the corresponding active user."""
    token = credentials.credentials
    payload = verify_token(token)  # noqa: F821 - JWT verification defined elsewhere
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = user_collection.find_one({"_id": ObjectId(payload["sub"])})  # noqa: F821
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    if not user.get("is_active", True):
        raise HTTPException(status_code=401, detail="User not found or inactive")

    return _serialize_doc_ids(user)  # noqa: F821


async def get_current_org_context(user=Depends(get_current_user)):
    """
    Resolve org, effective permissions, and subscription/billing state in a
    single dependency, exactly once per request. Every route that needs
    tenant-scoped access depends on this instead of re-deriving org_id from
    the user object independently.

    Super Admin users deliberately fail this check — they operate outside any
    single tenant and must use separate super-admin-only endpoints.
    """
    if user.get("role") == "super_admin":
        raise HTTPException(
            status_code=403, detail="Super Admin should use super admin endpoints"
        )

    org_id = user.get("organization_id")
    if not org_id:
        raise HTTPException(
            status_code=400,
            detail="User does not belong to an organization. Contact Super Admin.",
        )

    # Re-verify the organization still exists and is active on every request
    # rather than trusting a cached/stale value from the JWT.
    org = organizations_collection.find_one({"_id": org_id})  # noqa: F821
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    if not org.get("is_active", True):
        raise HTTPException(status_code=403, detail="Organization is deactivated")

    plan_limits = get_effective_org_plan_features(org)  # noqa: F821
    access_state = evaluate_org_subscription_access(org)  # noqa: F821

    # Auto-downgrade access if a trial has expired, evaluated fresh each request
    # rather than relying on a background job to have already flipped a flag.
    if access_state.get("phase") == "trial_expired":
        from services.rbac_service import apply_trial_expiry_role_to_org
        apply_trial_expiry_role_to_org(org_id)

    effective_access = get_effective_access_context(user, org)  # noqa: F821

    return {
        "user": _serialize_doc_ids(user),  # noqa: F821
        "user_id": str(user["_id"]),
        "org_id": org_id,  # kept as ObjectId for backend queries
        "role": user.get("role"),
        "organization": _serialize_doc_ids(org),  # noqa: F821
        "plan_limits": plan_limits,
        "effective_access": effective_access,
        "is_read_only": access_state.get("read_only", False),
        "subscription_phase": access_state.get("phase", "none"),
        "read_only_reason": access_state.get("reason"),
    }


def ensure_permission(context: dict, permission: str, detail: Optional[str] = None):
    """Raise 403 unless the resolved context's effective access grants `permission`."""
    if not has_permission(context.get("effective_access") or {}, permission):  # noqa: F821
        raise HTTPException(status_code=403, detail=detail or "Access denied")


def require_permission(permission: str, detail: Optional[str] = None) -> Callable:
    """FastAPI dependency factory: bundles org-context resolution + a permission check."""
    async def dependency(context=Depends(get_current_org_context)):
        ensure_permission(context, permission, detail)
        return True

    return dependency


def ensure_write_access(context: dict, action: str = "perform this action"):
    """Raise 403 if the org is in a read-only billing/subscription state."""
    if context.get("is_read_only"):
        raise HTTPException(
            status_code=403,
            detail=context.get("read_only_reason") or f"Cannot {action}: account is read-only",
        )
