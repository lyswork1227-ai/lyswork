import json
import unittest

import public_server
from starlette.websockets import WebSocketDisconnect


class FakeWebSocket:
	def __init__(self, messages):
		self.messages = list(messages)
		self.sent_json = []
		self.sent_text = []
		self.closed = []

	async def accept(self):
		pass

	async def receive_text(self):
		if not self.messages:
			raise WebSocketDisconnect(code=1000)
		return self.messages.pop(0)

	async def send_json(self, message):
		self.sent_json.append(message)

	async def send_text(self, message):
		self.sent_text.append(message)

	async def close(self, code, reason):
		self.closed.append((code, reason))


class ViewerAuthorizationTests(unittest.IsolatedAsyncioTestCase):
	def setUp(self):
		self.original_view_token = public_server.VIEW_TOKEN
		self.original_viewers = set(public_server.viewers)
		self.original_publishers = set(public_server.publishers)
		self.original_snapshot = public_server.latest_snapshot
		public_server.VIEW_TOKEN = 'local-test-view-token'
		public_server.viewers.clear()
		public_server.publishers.clear()
		public_server.latest_snapshot = json.dumps({
			'type': 'snapshot',
			'rows': [{'symbol': 'TEST-ONLY'}],
		})

	def tearDown(self):
		public_server.VIEW_TOKEN = self.original_view_token
		public_server.viewers.clear()
		public_server.viewers.update(self.original_viewers)
		public_server.publishers.clear()
		public_server.publishers.update(self.original_publishers)
		public_server.latest_snapshot = self.original_snapshot

	async def test_valid_viewer_token_receives_ready_and_snapshot(self):
		websocket = FakeWebSocket([json.dumps({
			'type': 'auth',
			'role': 'viewer',
			'token': 'local-test-view-token',
		})])

		await public_server.view_socket(websocket)

		self.assertEqual(websocket.sent_json, [{'type': 'ready', 'role': 'viewer'}])
		self.assertEqual(json.loads(websocket.sent_text[0]), {
			'type': 'snapshot',
			'rows': [{'symbol': 'TEST-ONLY'}],
		})
		self.assertEqual(json.loads(websocket.sent_text[1]), {
			'type': 'publisher_status',
			'connected': False,
		})
		self.assertEqual(websocket.closed, [])

	async def test_missing_invalid_and_wrong_role_credentials_are_rejected(self):
		cases = (
			{'type': 'auth', 'role': 'viewer'},
			{'type': 'auth', 'role': 'viewer', 'token': ''},
			{'type': 'auth', 'role': 'viewer', 'token': 'wrong-local-token'},
			{'type': 'auth', 'role': 'publisher', 'token': 'local-test-view-token'},
			{'type': 'snapshot', 'role': 'viewer', 'token': 'local-test-view-token'},
		)
		for message in cases:
			with self.subTest(message=message):
				websocket = FakeWebSocket([json.dumps(message)])

				await public_server.view_socket(websocket)

				self.assertEqual(websocket.sent_json, [{
					'type': 'error',
					'message': 'Authentication failed',
				}])
				self.assertEqual(websocket.closed, [(1008, 'Authentication failed')])
				self.assertEqual(websocket.sent_text, [])

	async def test_configured_empty_view_token_rejects_credentials(self):
		public_server.VIEW_TOKEN = ''
		for client_token in ('', 'local-test-view-token'):
			with self.subTest(client_token=bool(client_token)):
				websocket = FakeWebSocket([json.dumps({
					'type': 'auth',
					'role': 'viewer',
					'token': client_token,
				})])

				await public_server.view_socket(websocket)

				self.assertEqual(websocket.sent_json, [{
					'type': 'error',
					'message': 'Authentication failed',
				}])
				self.assertEqual(websocket.closed, [(1008, 'Authentication failed')])
				self.assertEqual(websocket.sent_text, [])


if __name__ == '__main__':
	unittest.main()
