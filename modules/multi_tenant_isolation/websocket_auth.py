"""
WebSocket connection authentication with double tenant-scope verification.

Adapted/excerpted from a production FastAPI backend for portfolio purposes.
Not runnable standalone (`verify_token`, `user_collection`, and the
connection `manager` are defined elsewhere in the original codebase).

The pattern: a WebSocket connection is only accepted after the org_id in the
URL is verified TWICE via two independent sources of truth — the JWT claim,
and a fresh database lookup of the user's actual organization membership.
Either mismatch closes the socket before any data can flow. This matters
specifically for WebSockets because, unlike a REST request, a single
successful handshake stays open and keeps receiving broadcast traffic for
the room it joined — so the accept-time check has to be right, since a
per-message secondary check like a REST middleware would provide is not
naturally available for an open connection.
"""

from fastapi import WebSocket, WebSocketDisconnect
from bson import ObjectId


async def websocket_endpoint(websocket: WebSocket, org_id: str):
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=1008)
        return

    payload = verify_token(token)  # noqa: F821 - JWT verification defined elsewhere
    if not payload:
        await websocket.close(code=1008)
        return

    user_id = payload.get("sub")
    token_org_id = payload.get("org_id")

    # Check 1: does the org_id embedded in the JWT match the org_id in the URL?
    if not user_id or not token_org_id or str(token_org_id) != str(org_id):
        await websocket.close(code=1008)
        return

    try:
        user = user_collection.find_one({"_id": ObjectId(user_id)})  # noqa: F821
    except Exception:
        user = None

    if not user or not user.get("is_active", True):
        await websocket.close(code=1008)
        return

    # Check 2: independent of the JWT claim, does the user's *current* DB
    # record actually belong to this org? Catches a stale/replayed token for
    # a user who has since been moved to a different organization.
    if str(user.get("organization_id")) != str(org_id):
        await websocket.close(code=1008)
        return

    scoped_org_id = str(org_id)
    await manager.connect(websocket, scoped_org_id)  # noqa: F821 - ConnectionManager, see realtime_websocket module
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, scoped_org_id)  # noqa: F821
    except Exception:
        manager.disconnect(websocket, scoped_org_id)  # noqa: F821
