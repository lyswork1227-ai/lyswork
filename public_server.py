"""Public WebSocket relay and browser dashboard for Palmmicro."""

from __future__ import annotations

import asyncio
import hmac
import json
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse


MAX_MESSAGE_BYTES = 2 * 1024 * 1024
PUBLISH_TOKEN = os.getenv('PALMMICRO_PUBLISH_TOKEN', '')
VIEW_TOKEN = os.getenv('PALMMICRO_VIEW_TOKEN', '')
INDEX_HTML = Path(__file__).with_name('web').joinpath('index.html')
if not INDEX_HTML.exists():
	INDEX_HTML = Path(__file__).with_name('index.html')

app = FastAPI(title='Palmmicro Live', docs_url=None, redoc_url=None)
latest_snapshot: Optional[str] = None
viewers: set[WebSocket] = set()
publisher_connected = False


def _token_matches(received: object, expected: str) -> bool:
	return bool(expected) and isinstance(received, str) and hmac.compare_digest(received, expected)


async def _authenticate(websocket: WebSocket, role: str, token: str) -> bool:
	try:
		raw = await asyncio.wait_for(websocket.receive_text(), timeout=10)
		message = json.loads(raw)
	except (asyncio.TimeoutError, json.JSONDecodeError, WebSocketDisconnect):
		await websocket.close(code=1008, reason='Authentication required')
		return False

	if (
		message.get('type') != 'auth'
		or message.get('role') != role
		or not _token_matches(message.get('token'), token)
	):
		await websocket.send_json({'type': 'error', 'message': 'Authentication failed'})
		await websocket.close(code=1008, reason='Authentication failed')
		return False

	await websocket.send_json({'type': 'ready', 'role': role})
	return True


async def _broadcast(payload: str):
	stale: list[WebSocket] = []
	for viewer in tuple(viewers):
		try:
			await viewer.send_text(payload)
		except Exception:
			stale.append(viewer)
	for viewer in stale:
		viewers.discard(viewer)


@app.get('/')
async def index():
	return HTMLResponse(INDEX_HTML.read_text(encoding='utf-8'))


@app.get('/health')
async def health():
	return JSONResponse({
		'ok': True,
		'publisher_connected': publisher_connected,
		'viewer_count': len(viewers),
		'has_snapshot': latest_snapshot is not None,
	})


@app.websocket('/ws/publish')
async def publish_socket(websocket: WebSocket):
	global latest_snapshot, publisher_connected
	await websocket.accept()
	if not await _authenticate(websocket, 'publisher', PUBLISH_TOKEN):
		return

	publisher_connected = True
	try:
		while True:
			payload = await websocket.receive_text()
			if len(payload.encode('utf-8')) > MAX_MESSAGE_BYTES:
				await websocket.close(code=1009, reason='Snapshot too large')
				return
			message = json.loads(payload)
			if message.get('type') != 'snapshot' or not isinstance(message.get('rows'), list):
				continue
			latest_snapshot = payload
			await _broadcast(payload)
	except (WebSocketDisconnect, json.JSONDecodeError):
		pass
	finally:
		publisher_connected = False
		await _broadcast(json.dumps({'type': 'publisher_status', 'connected': False}))


@app.websocket('/ws/view')
async def view_socket(websocket: WebSocket):
	await websocket.accept()
	if not await _authenticate(websocket, 'viewer', VIEW_TOKEN):
		return

	viewers.add(websocket)
	if latest_snapshot is not None:
		await websocket.send_text(latest_snapshot)
	try:
		while True:
			message = await websocket.receive_text()
			if message == 'ping':
				await websocket.send_text('{"type":"pong"}')
	except WebSocketDisconnect:
		pass
	finally:
		viewers.discard(websocket)
