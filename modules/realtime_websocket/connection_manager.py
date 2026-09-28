"""
Org-scoped WebSocket connection manager for a real-time inbox.

Copied near-verbatim from a production codebase for portfolio purposes (this
file was already small and self-contained). See
`modules/multi_tenant_isolation/websocket_auth.py` for the connection-time
auth check that runs before `connect()` is ever called — this manager
intentionally trusts the org_id it's given, because that boundary is
enforced upstream.
"""

from typing import Dict, List
from fastapi import WebSocket
import logging

logger = logging.getLogger(__name__)
SUPER_ADMIN_CHANNEL = "__super_admin__"


class ConnectionManager:
    def __init__(self):
        # Map org_id -> List[WebSocket]. In-memory, single-process by design;
        # a multi-instance deployment would need a shared layer (e.g. Redis
        # pub/sub) to fan out across processes — not implemented here.
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, org_id: str):
        await websocket.accept()
        if org_id not in self.active_connections:
            self.active_connections[org_id] = []
        self.active_connections[org_id].append(websocket)
        logger.info(f"WebSocket connected for Org: {org_id}")

    def disconnect(self, websocket: WebSocket, org_id: str):
        if org_id in self.active_connections:
            if websocket in self.active_connections[org_id]:
                self.active_connections[org_id].remove(websocket)
            if not self.active_connections[org_id]:
                del self.active_connections[org_id]
        logger.info(f"WebSocket disconnected for Org: {org_id}")

    async def broadcast(self, message: dict, org_id: str):
        if org_id in self.active_connections:
            # Iterate a copy so a mid-broadcast disconnect doesn't mutate the
            # list we're iterating.
            for connection in self.active_connections[org_id][:]:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    logger.error(f"Error sending to websocket: {e}")
                    self.disconnect(connection, org_id)


# Global instance
manager = ConnectionManager()
