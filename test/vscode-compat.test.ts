import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import test from 'node:test';
import {
  ActionType,
  PROTOCOL_VERSION,
  SessionStatus,
  type ActionEnvelope,
  type StateAction,
} from '@microsoft/agent-host-protocol';
import { AhpClient, type Subscription } from '@microsoft/agent-host-protocol/client';
import { WebSocketTransport } from '@microsoft/agent-host-protocol/ws';
import { GatewayServer } from '../src/gateway.js';
import type { GatewayConfig } from '../src/config.js';

const fakeAgent = fileURLToPath(new URL('./fake-agent.ts', import.meta.url));

// The offer VS Code 1.147.0 sends on every non-root `subscribe`
// (`{ windowBytes: 1024 * 1024, maxFrameBytes: 16 * 1024 * 1024 }` in its workbench bundle).
const vscodeReceiveLimits = { windowBytes: 1024 * 1024, maxFrameBytes: 16 * 1024 * 1024 };

interface Fixture {
  server: GatewayServer;
  url: string;
  close: () => Promise<void>;
}

async function startFixture(providerCommand = process.execPath, providerArgs = ['--import', 'tsx', fakeAgent]): Promise<Fixture> {
  const directory = await mkdtemp(join(tmpdir(), 'ahp-acp-gateway-compat-'));
  const config: GatewayConfig = {
    version: 1,
    listener: {
      host: '127.0.0.1',
      port: 0,
      path: '/ahp',
      tlsTerminated: false,
      allowedOrigins: [],
      allowAnyOrigin: false,
    },
    auth: {},
    database: { path: join(directory, 'gateway.db') },
    replay: { maxActions: 100 },
    providerDefaults: {
      startupTimeoutMs: 15_000,
      requestTimeoutMs: 300_000,
      shutdownTimeoutMs: 5_000,
      stderrTailBytes: 16_384,
    },
    providers: [{
      id: 'fake',
      displayName: 'Fake ACP',
      description: 'Official ACP SDK test agent',
      command: providerCommand,
      args: providerArgs,
      startupTimeoutMs: 10_000,
      requestTimeoutMs: 10_000,
      shutdownTimeoutMs: 2_000,
      stderrTailBytes: 8_192,
    }],
  };
  const server = new GatewayServer(config);
  const address = await server.listen();
  return {
    server,
    url: `ws://${address.host}:${address.port}${address.path}`,
    close: async () => {
      await server.close();
      await rm(directory, { recursive: true, force: true });
    },
  };
}

async function connectClient(url: string, id: string): Promise<AhpClient> {
  const client = new AhpClient(await WebSocketTransport.connect(url), { requestTimeoutMs: 10_000 });
  client.connect();
  await client.initialize({
    clientId: id,
    protocolVersions: [PROTOCOL_VERSION],
    initialSubscriptions: ['ahp-root://'],
  });
  return client;
}

async function createSessionResource(client: AhpClient, name: string, sessionUri?: string) {
  const session = sessionUri ?? `ahp-session:/${name}-${crypto.randomUUID()}`;
  await client.request('createSession', {
    channel: session,
    provider: 'fake',
    workingDirectories: [pathToFileURL(process.cwd()).href],
  });
  const state = (await client.subscribe(session)).result.snapshot?.state;
  assert.ok(state && 'chats' in state);
  const chat = state.defaultChat;
  assert.ok(chat);
  return { session, chat };
}

function userTurn(turnId: string, text: string): StateAction {
  return {
    type: ActionType.ChatTurnStarted,
    turnId,
    startedAt: new Date().toISOString(),
    message: {
      text,
      origin: { kind: 'user' },
    },
  } as StateAction;
}

function withDeadline<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timed out after ${timeoutMs} ms waiting for AHP data`)),
      timeoutMs,
    );
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

async function nextAction(
  subscription: Subscription,
  predicate: (envelope: ActionEnvelope) => boolean,
): Promise<ActionEnvelope> {
  for (;;) {
    const result = await withDeadline(subscription.next(), 15_000);
    if (result.done) throw new Error('Subscription closed');
    if (result.value.type === 'action' && predicate(result.value.params)) return result.value.params;
  }
}

async function waitForCondition(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('Timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

function expectRpcCode(expectedCode: number) {
  return (error: unknown) => {
    assert.equal((error as { code?: number }).code, expectedCode, String(error));
    return true;
  };
}

// Emulates the raw `subscribe` request VS Code sends; the reference client's
// `subscribe()` helper never offers windowed delivery.
function subscribeWithOffer(
  client: AhpClient,
  channel: string,
  flowControl: unknown = { receive: vscodeReceiveLimits },
) {
  const params: Record<string, unknown> = { channel };
  if (flowControl !== undefined) params.flowControl = flowControl;
  return client.request('subscribe', params as never);
}

function readBit(status: number | undefined): number {
  return (status ?? 0) & SessionStatus.IsRead;
}

test('A1 subscribe without a flowControl offer returns an ordinary snapshot and delivers later actions', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'a1');
  t.after(() => client.shutdown());
  const { session, chat } = await createSessionResource(client, 'a1');
  for (const channel of [session, chat]) {
    const params = { channel };
    const result = await client.request('subscribe', params);
    // `flowControl` on the result exists from AHP 1.1.0; the lockfile pins 1.0.0.
    assert.equal((result as { flowControl?: unknown }).flowControl, undefined);
    assert.equal(result.snapshot?.resource, channel);
  }
  const subscription = client.attachSubscription(chat);
  client.dispatch(chat, userTurn(crypto.randomUUID(), 'hello'));
  const complete = await nextAction(subscription, (item) => item.action.type === ActionType.ChatTurnComplete);
  assert.ok(complete.serverSeq > 0);
});

test('A2 subscribe with the VS Code flowControl offer falls back to ordinary delivery without -32602', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'a2');
  t.after(() => client.shutdown());
  const { session, chat } = await createSessionResource(client, 'a2');
  for (const channel of [session, chat]) {
    const result = await subscribeWithOffer(client, channel);
    assert.equal(
      (result as { flowControl?: unknown }).flowControl,
      undefined,
      'windowed delivery is not implemented, so it must not be accepted',
    );
    assert.equal(result.snapshot?.resource, channel, 'ordinary delivery returns the snapshot');
  }
  const subscription = client.attachSubscription(chat);
  client.dispatch(chat, userTurn(crypto.randomUUID(), 'hello'));
  const actions: ActionEnvelope[] = [];
  const complete = await nextAction(subscription, (item) => {
    actions.push(item);
    return item.action.type === ActionType.ChatTurnComplete;
  });
  assert.ok(actions.length >= 2);
  assert.ok(complete.serverSeq > 0);
});

test('A3 unknown channels and malformed flowControl offers still fail with their spec-defined errors', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'a3');
  t.after(() => client.shutdown());
  const { chat } = await createSessionResource(client, 'a3');
  await assert.rejects(
    client.request('subscribe', { channel: 'ahp-session:/does-not-exist' }),
    expectRpcCode(-32001),
  );
  await assert.rejects(
    subscribeWithOffer(client,'ahp-session:/does-not-exist'),
    expectRpcCode(-32001),
    'an unknown channel is reported as not found, not as a bad offer',
  );
  for (const malformed of [
    'windowed',
    true,
    {},
    { receive: 'unlimited' },
    { receive: {} },
    { receive: { windowBytes: 0, maxFrameBytes: 1024 } },
    { receive: { windowBytes: 1024, maxFrameBytes: 1023 } },
    { receive: { windowBytes: 1.5, maxFrameBytes: 1024 } },
    { receive: { windowBytes: 1048576, maxFrameBytes: '16777216' } },
  ]) {
    await assert.rejects(
      subscribeWithOffer(client,chat, malformed),
      (error: unknown) => {
        assert.equal((error as { code?: number }).code, -32602, `offer ${JSON.stringify(malformed)}: ${String(error)}`);
        return true;
      },
      `malformed offer ${JSON.stringify(malformed)} must be rejected as invalid params`,
    );
  }
});

test('A4 re-subscribe, unsubscribe and reconnect keep ordinary delivery consistent when offers are used', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'a4');
  t.after(() => client.shutdown());
  const { chat } = await createSessionResource(client, 'a4');
  const first = await subscribeWithOffer(client, chat);
  const again = await subscribeWithOffer(client, chat);
  assert.deepEqual(again.snapshot?.fromSeq, first.snapshot?.fromSeq, 're-subscribe does not replay or duplicate');
  await client.unsubscribe(chat);
  const subscription = client.attachSubscription(chat);
  const after = await subscribeWithOffer(client, chat);
  assert.ok(after.snapshot, 'subscribe after unsubscribe returns a snapshot');
  const turnId = crypto.randomUUID();
  client.dispatch(chat, userTurn(turnId, 'hello'));
  const seen: number[] = [];
  await nextAction(subscription, (item) => {
    seen.push(item.serverSeq);
    return item.action.type === ActionType.ChatTurnComplete;
  });
  assert.deepEqual(seen, [...new Set(seen)], 'each action is delivered exactly once');
  const replay = await client.reconnect({
    clientId: 'a4',
    lastSeenServerSeq: after.snapshot?.fromSeq ?? 0,
    subscriptions: [chat],
  });
  assert.equal(replay.type, 'replay');
  if (replay.type === 'replay') {
    const replayedSeqs = replay.actions.map((item) => item.serverSeq);
    assert.deepEqual(replayedSeqs, [...new Set(replayedSeqs)], 'replay never duplicates an action');
    assert.deepEqual(
      replayedSeqs.filter((seq) => seen.includes(seq)),
      seen,
      'reconnect replays the retained turn actions in order',
    );
  }
  const resubscribed = await subscribeWithOffer(client, chat);
  assert.ok(resubscribed.snapshot, 'subscribe after reconnect returns an ordinary snapshot');
});

test('B-spec createSession for an existing session URI still returns SessionAlreadyExists', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'b-spec');
  t.after(() => client.shutdown());
  const sessionUri = `ahp-session:/vscode-chat-${crypto.randomUUID()}`;
  const { chat } = await createSessionResource(client, 'b-spec', sessionUri);
  await assert.rejects(
    client.request('createSession', {
      channel: sessionUri,
      provider: 'fake',
      workingDirectories: [pathToFileURL(process.cwd()).href],
    }),
    expectRpcCode(-32003),
    'the spec requires -32003 when the session URI already exists',
  );
  const state = (await client.subscribe(sessionUri)).result.snapshot?.state as { lifecycle?: string };
  assert.equal(state.lifecycle, 'ready');
  assert.ok(chat);
});

test('B-flow the VS Code create -> subscribe(offered) -> prompt sequence completes without re-creating the session', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'b-flow');
  t.after(() => client.shutdown());
  const sessionUri = `ahp-session:/vscode-chat-${crypto.randomUUID()}`;
  const created = await client.request('createSession', {
    channel: sessionUri,
    provider: 'fake',
    workingDirectories: [pathToFileURL(process.cwd()).href],
  });
  assert.equal(created, null);
  const sessionState = (await subscribeWithOffer(client, sessionUri)).snapshot?.state as { defaultChat?: string; lifecycle?: string };
  assert.equal(sessionState.lifecycle, 'ready');
  const chat = sessionState.defaultChat;
  assert.ok(chat);
  const subscription = client.attachSubscription(chat);
  await subscribeWithOffer(client, chat);
  client.dispatch(chat, userTurn(crypto.randomUUID(), 'hello'));
  const complete = await nextAction(subscription, (item) => item.action.type === ActionType.ChatTurnComplete);
  assert.ok(complete.serverSeq > 0);
  assert.equal(fixture.server.store.listSessions(10, 0).length, 1, 'one session exists, so no retry can hit SessionAlreadyExists');
});

test('B2 a failed provider start keeps the failed session and a blind retry reports SessionAlreadyExists until it is disposed', async (t) => {
  const fixture = await startFixture(join(tmpdir(), 'no-such-agent-executable'));
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'b2');
  t.after(() => client.shutdown());
  const sessionUri = `ahp-session:/vscode-chat-${crypto.randomUUID()}`;
  const create = () => client.request('createSession', {
    channel: sessionUri,
    provider: 'fake',
    workingDirectories: [pathToFileURL(process.cwd()).href],
  });
  await assert.rejects(create(), (error: unknown) => {
    assert.equal((error as { code?: number }).code, -32603);
    assert.match((error as Error).message, /was not found/);
    return true;
  }, 'the provider failure itself is reported');
  const failed = (await client.subscribe(sessionUri)).result.snapshot?.state as { lifecycle?: string; creationError?: { errorType?: string } };
  assert.equal(failed.lifecycle, 'failed');
  assert.equal(failed.creationError?.errorType, 'provider_start_failed');
  await assert.rejects(create(), expectRpcCode(-32003), 'a retry against the retained failed session reports SessionAlreadyExists');
  await client.request('disposeSession', { channel: sessionUri });
  await assert.rejects(create(), expectRpcCode(-32603), 'after disposal the real provider failure is reported again');
});

test('C1 chat read marks are acknowledged and projected into the chat catalog', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'c1');
  t.after(() => client.shutdown());
  const { session, chat } = await createSessionResource(client, 'c1');
  const chatSubscription = (await client.subscribe(chat)).subscription;
  const sessionSubscription = client.attachSubscription(session);
  await client.dispatch(chat, { type: ActionType.ChatIsReadChanged, isRead: true } as StateAction);
  const ack = await nextAction(chatSubscription, (item) => item.action.type === ActionType.ChatIsReadChanged);
  assert.equal(ack.rejectionReason, undefined);
  assert.deepEqual(ack.origin, { clientId: 'c1', clientSeq: 1 });
  const projected = await nextAction(
    sessionSubscription,
    (item) => item.action.type === ActionType.SessionChatUpdated && item.action.chat === chat,
  );
  assert.equal((projected.action as { changes: { status: number } }).changes.status & SessionStatus.IsRead, SessionStatus.IsRead);
  assert.notEqual(readBit(fixture.server.store.getChat(chat)?.state.status), 0);
});

test('C2 VS Code row-open reconciliation settles after one chat read mark', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'c2');
  t.after(() => client.shutdown());
  const { session, chat } = await createSessionResource(client, 'c2');
  const chatSubscription = (await client.subscribe(chat)).subscription;
  client.dispatch(chat, userTurn(crypto.randomUUID(), 'hello'));
  await nextAction(chatSubscription, (item) => item.action.type === ActionType.ChatTurnComplete);

  const summaries: Array<number | undefined> = [];
  const rootSubscription = client.attachSubscription('ahp-root://');
  let pumping = true;
  const pump = (async () => {
    try {
      while (pumping) {
        const event = await rootSubscription.next();
        if (event.done) return;
        if (event.value.type === 'sessionSummaryChanged' && event.value.params.session === session) {
          summaries.push(event.value.params.changes.status);
        }
      }
    } catch {
      // The subscription closes when the test finishes.
    }
  })();
  t.after(async () => {
    pumping = false;
    await rootSubscription.close();
    await pump;
  });

  let iterations = 0;
  let settled = false;
  while (!settled && iterations < 4) {
    iterations += 1;
    const before = summaries.length;
    client.dispatch(chat, { type: ActionType.ChatIsReadChanged, isRead: true } as StateAction);
    await waitForCondition(() => summaries.length > before);
    settled = ((summaries.at(-1) ?? 0) & SessionStatus.IsRead) !== 0;
  }
  assert.equal(iterations, 1, `the session row stayed unread after ${iterations} chat read marks`);
  assert.equal(settled, true, 'the session summary reports the row as read');
});

test('C4 an explicit session read mark is acknowledged and published', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'c4');
  t.after(() => client.shutdown());
  const { session, chat } = await createSessionResource(client, 'c4');
  const sessionSubscription = client.attachSubscription(session);
  client.dispatch(session, { type: ActionType.SessionIsReadChanged, isRead: true } as StateAction);
  const ack = await nextAction(sessionSubscription, (item) => item.action.type === ActionType.SessionIsReadChanged);
  assert.equal(ack.rejectionReason, undefined);
  assert.notEqual(readBit(fixture.server.store.getSession(session)?.state.status), 0);
  assert.equal(readBit(fixture.server.store.getChat(chat)?.state.status), 0, 'the chat keeps its own read state');
  const listed = await client.request('listSessions', { channel: 'ahp-root://' });
  assert.notEqual(readBit(listed.items[0]?.status), 0);
});

test('D1 twenty sequential turns complete and leave the chat idle', async (t) => {
  const fixture = await startFixture();
  t.after(fixture.close);
  const client = await connectClient(fixture.url, 'd1');
  t.after(() => client.shutdown());
  const { chat } = await createSessionResource(client, 'd1');
  const subscription = (await client.subscribe(chat)).subscription;
  for (let index = 0; index < 20; index += 1) {
    client.dispatch(chat, userTurn(crypto.randomUUID(), `turn ${index}`));
    await nextAction(subscription, (item) => item.action.type === ActionType.ChatTurnComplete);
  }
  const state = fixture.server.store.getChat(chat)?.state;
  assert.equal(state?.activeTurn, undefined);
  assert.equal(state?.turns.length, 20);
  assert.ok(state?.turns.every((turn) => turn.state !== 'error'));
});
