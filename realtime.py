from typing import Dict, List
from fastapi import WebSocket, WebSocketDisconnect

class TripWSManager:
    def __init__(self):
        # trip_id -> list of websockets
        self.active_connections: Dict[int, List[WebSocket]] = {}

    async def connect(self, trip_id: int, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.setdefault(trip_id, []).append(websocket)

    def disconnect(self, trip_id: int, websocket: WebSocket):
        if trip_id in self.active_connections:
            if websocket in self.active_connections[trip_id]:
                self.active_connections[trip_id].remove(websocket)
            if not self.active_connections[trip_id]:
                del self.active_connections[trip_id]

    async def broadcast_to_trip(self, trip_id: int, message: str):
        connections = self.active_connections.get(trip_id, [])
        to_remove = []
        for ws in connections:
            try:
                await ws.send_text(message)
            except Exception:
                to_remove.append(ws)

        for ws in to_remove:
            self.disconnect(trip_id, ws)

ws_manager = TripWSManager()
