"""In-memory hub for live WebSocket clients of one server process.

Each client gets a bounded queue; a slow/dead client never blocks the server.
When the queue overflows the oldest messages are dropped and a fresh snapshot
(resynchronisation) is pushed instead.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from fastapi import WebSocket


@dataclass(eq=False)  # identity hash: clients are keyed by object, not value
class Client:
    ws: WebSocket
    queue: asyncio.Queue[dict[str, Any] | None] = field(default_factory=lambda: asyncio.Queue(maxsize=32))
    role_id: str | None = None       # None => presenter
    resync: bool = False             # queue overflowed; send fresh snapshot next


class Hub:
    def __init__(self) -> None:
        self._sessions: dict[str, set[Client]] = {}

    def add(self, session_id: str, client: Client) -> None:
        self._sessions.setdefault(session_id, set()).add(client)

    def remove(self, session_id: str, client: Client) -> None:
        group = self._sessions.get(session_id)
        if group:
            group.discard(client)

    def online_roles(self, session_id: str) -> dict[str, bool]:
        out: dict[str, bool] = {}
        for c in self._sessions.get(session_id, ()):  # type: ignore[arg-type]
            if c.role_id:
                out[c.role_id] = True
        return out

    def push(self, session_id: str, message: dict[str, Any]) -> None:
        for c in list(self._sessions.get(session_id, ())):  # type: ignore[arg-type]
            self._enqueue(c, message)

    def push_to(self, client: Client, message: dict[str, Any]) -> None:
        self._enqueue(client, message)

    @staticmethod
    def _enqueue(c: Client, message: dict[str, Any]) -> None:
        try:
            c.queue.put_nowait(message)
        except asyncio.QueueFull:
            # Drop the whole backlog; the sender will emit a resync snapshot.
            while not c.queue.empty():
                try:
                    c.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            c.resync = True
            try:
                c.queue.put_nowait({"type": "resync"})
            except asyncio.QueueFull:
                pass
