const { test, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { dateOnly, eventId, markdown, loadEvents, createClient, sync } = require('./index');

const now = Date.parse('2026-10-21T12:00:00Z');
beforeEach(context => context.mock.method(Date, 'now', () => now));

const event = { summary: 'Car Meet', dtstart: '2026-10-19T00:30:00+02:00',
  dtend: '2026-10-20T18:00:00+02:00', location: 'Budapest',
  url: 'https://example.com/event', description: '<b>Welcome</b>', geo: '47.5;19.0' };
const prepared = loadEvents([{ source: event }])[0];

/** Mock the project API at its operation boundary and retain every mutation. */
function fixture(items = [], options = {}) {
  const calls = [];
  const requests = [];
  const fields = ['Start', 'End', 'Location', 'ID', 'Status'].map(name => ({
    id: name, name, dataType: ['Start', 'End'].includes(name) ? 'DATE'
      : name === 'Status' ? 'SINGLE_SELECT' : 'TEXT',
    options: ['Todo', 'Old', 'Done'].map(value => ({ id: value, name: value })),
  }));
  const api = async (query, variables) => {
    const operation = query.match(/(?:query|mutation) (\w+)/)[1];
    requests.push({ query, variables });
    if (operation === 'Batch') {
      const operations = { addProjectV2DraftIssue: 'Create', updateProjectV2DraftIssue: 'Refresh',
        updateProjectV2ItemFieldValue: 'Set', clearProjectV2ItemFieldValue: 'Clear',
        deleteProjectV2Item: 'Delete' };
      const result = {};
      for (const match of query.matchAll(/(m\d+): (\w+)\(input: \$(input\d+)\)/g)) {
        const [, alias, field, variable] = match;
        calls.push({ operation: operations[field], input: variables[variable] });
        const id = options.uniqueIds ? `created-${calls.length}` : 'created';
        result[alias] = field === 'addProjectV2DraftIssue'
          ? { projectItem: { id } } : {};
      }
      return result;
    }
    calls.push({ operation, ...variables });
    const connection = nodes => ({ nodes, pageInfo: { hasNextPage: false, endCursor: null } });
    if (operation === 'Project') return { repositoryOwner: { projectV2: { id: 'project' } } };
    if (operation === 'Fields') {
      const available = fields.filter(field => field.name !== options.missing);
      return { node: { fields: connection(available) } };
    }
    if (operation === 'Items') {
      if (options.paginate && !variables.cursor) {
        return { node: { items: { nodes: [],
          pageInfo: { hasNextPage: true, endCursor: 'next' } } } };
      }
      return { node: { items: connection(items) } };
    }
    if (operation === 'Create') {
      return { addProjectV2DraftIssue: { projectItem: { id: 'created' } } };
    }
    return {};
  };
  return { api, calls, requests };
}

function item(id, status, suffix = status) {
  return { id: suffix, eventId: { text: id }, status: { name: status },
    content: { __typename: 'DraftIssue', id: `draft-${suffix}`, body: '' } };
}

test('identity ignores accents, punctuation, spacing, times, sources and extra URLs', () => {
  const first = { ...event, summary: 'Aut\u00f3s Tal\u00e1lkoz\u00f3!' };
  const changed = { ...first, summary: 'AUTOS-talalkozo',
    dtstart: '2026-10-19T20:00:00Z', dtend: null, src_id: 'extra', url: 'https://other.test' };
  assert.equal(eventId(first), eventId(changed));
  for (const change of [{ summary: 'Other' }, { dtstart: '2026-10-20' }, { location: 'Zirc' }]) {
    assert.notEqual(eventId(first), eventId({ ...first, ...change }));
  }
  assert.equal(dateOnly(event.dtstart), '2026-10-19');
  assert.equal(dateOnly('2026-02-30'), null);
  assert.equal(dateOnly('broken'), null);
});

test('Markdown preserves emoji, formatting and links, with URLs before description', () => {
  const text = '<p>Hello &#x1F697; &amp; <b>bold</b> <i>italic</i></p>'
    + '<a href="https://example.com/a(b)?x=1&amp;y=2">Visit</a><br>**Already Markdown**';
  const converted = markdown(text);
  assert.match(converted, /Hello \u{1F697} & \*\*bold\*\* \*italic\*/u);
  assert.match(converted, /\[Visit\]\(<https:\/\/example.com\/a\(b\)\?x=1&y=2>\)/);
  assert.match(converted, /\*\*Already Markdown\*\*/);
  assert.equal(markdown('<script>bad()</script><a href="javascript:bad()">Safe</a>'), 'Safe');
  assert.ok(prepared.body.indexOf(event.url) < prepared.body.indexOf('## Description'));
  assert.match(prepared.body, /47\.5;19\.0/);
  assert.match(prepared.body, /\*\*Src Id:\*\* source/);
});

test('merged URLs are deduplicated and placed before the description', () => {
  const urls = '<a href="https://first.test/">https://first.test/</a>\n'
    + '<a href="https://second.test/">https://second.test/</a>';
  const merged = loadEvents([{ sources: { ...event, url: '', urls } }])[0];
  for (const url of ['https://first.test/', 'https://second.test/']) {
    assert.equal(merged.body.split(url).length - 1, 1);
    assert.ok(merged.body.indexOf(url) < merged.body.indexOf('## Description'));
  }
});

test('required data fails before syncing; invalid optional end dates can be cleared', () => {
  assert.throws(() => loadEvents({}), /array/);
  assert.throws(() => loadEvents([{ source: { summary: 'Missing date' } }]), /valid start/);
  assert.equal(dateOnly(loadEvents([{ source: { ...event, dtend: 'bad' } }])[0].dtend), null);
});

test('creates drafts with Todo, dates and ID but no Assignees', async () => {
  const { api, calls } = fixture();
  const report = await sync(api, 'owner', 1, [prepared]);
  assert.deepEqual(report, { events: [{ event_id: prepared.id, item_id: 'created',
      summary: event.summary, start: event.dtstart, end: '2026-10-20', location: 'Budapest',
      is_new: true, old_status: null, new_status: 'Todo' }] });
  const created = calls.find(call => call.operation === 'Create');
  assert.equal(created.input.title, event.summary);
  assert.match(created.input.body, /<!-- sync-project:event-v1-/);
  const updates = calls.filter(call => call.operation === 'Set');
  assert.deepEqual(updates.map(call => call.input.fieldId),
    ['Status', 'ID', 'Start', 'End', 'Location']);
  assert.deepEqual(updates.at(0).input.value, { singleSelectOptionId: 'Todo' });
  assert.equal(updates[2].input.value.date, '2026-10-19');
  assert.ok(!JSON.stringify(calls).includes('assignee'));
});

test('refreshes returning Old, preserves Done, and marks only absent Todo items Old', async () => {
  const done = { ...prepared, id: eventId({ ...event, summary: 'Done event' }) };
  const absent = eventId({ ...event, summary: 'Absent' });
  const absentDone = eventId({ ...event, summary: 'Absent done' });
  const { api, calls } = fixture([item(prepared.id, 'Old'), item(done.id, 'Done'),
    item(absent, 'Todo'), item(absentDone, 'Done', 'absent-done')], { paginate: true });
  const report = await sync(api, 'owner', 1, [prepared, done]);
  assert.ok(report.events.every(change => !change.is_new));
  assert.deepEqual(report.events.map(change =>
    [change.is_new, change.old_status, change.new_status]),
  [[false, 'Old', 'Todo'], [false, 'Done', 'Done']]);
  assert.ok(!report.events.some(change => change.event_id === absent));
  assert.equal(calls.filter(call => call.operation === 'Items').length, 2);
  assert.equal(calls.filter(call => call.operation === 'Refresh').length, 2);
  assert.equal(calls.filter(call => call.operation === 'Create').length, 0);
  const statuses = calls.filter(call => call.input?.fieldId === 'Status');
  const transitions = statuses.map(call =>
    [call.input.itemId, call.input.value.singleSelectOptionId]);
  assert.deepEqual(transitions, [['Old', 'Todo'], ['Todo', 'Old']]);
});

test('recovers a partial draft, sets Todo and clears missing optional fields', async () => {
  const draft = item(prepared.id, null, 'unfinished');
  draft.eventId = null;
  draft.content.body = prepared.body;
  draft.end = { date: '2026-10-20' };
  draft.location = { text: 'Budapest' };
  const { api, calls } = fixture([draft]);
  const report = await sync(api, 'owner', 1,
    [{ ...prepared, dtend: 'invalid', location: '' }]);
  assert.equal(report.events[0].is_new, false);
  assert.equal(report.events[0].old_status, null);
  assert.equal(report.events[0].new_status, 'Todo');
  assert.ok(!calls.some(call => call.operation === 'Create'));
  const cleared = calls.filter(call => call.operation === 'Clear');
  assert.deepEqual(cleared.map(call => call.input.fieldId), ['End', 'Location']);
  assert.deepEqual(calls.find(call => call.input?.fieldId === 'Status').input.value,
    { singleSelectOptionId: 'Todo' });
});

test('ignores unrelated issues and drafts and fails on missing project fields before mutations',
  async () => {
    const unrelated = item('manual-id', 'Todo');
    const untagged = item(null, 'Todo', 'manual-draft');
    const issue = item(prepared.id, 'Todo', 'issue');
    issue.content.__typename = 'Issue';
    const { api, calls } = fixture([unrelated, untagged, issue]);
    await sync(api, 'owner', 1, []);
    assert.ok(!calls.some(call => call.input));
    const missing = fixture([], { missing: 'End' });
    await assert.rejects(sync(missing.api, 'owner', 1, []), /End with type DATE/);
    assert.ok(!missing.calls.some(call => call.input));
  });

test('GraphQL transport reports HTTP and GraphQL errors', async context => {
  const client = createClient('test');
  context.mock.method(globalThis, 'fetch', async () => ({ ok: false, status: 403 }));
  await assert.rejects(client('query {}', {}), /HTTP 403/);
  globalThis.fetch = async () => ({ ok: true, json: async () => ({ errors: ['denied'] }) });
  await assert.rejects(client('query {}', {}), /denied/);
});

test('19 new events need nine requests, with at most 20 mutations per request', async () => {
  const events = loadEvents(Array.from({ length: 19 }, (_, index) =>
    ({ source: { ...event, summary: `Meet ${index}` } })));
  const { api, requests } = fixture([], { uniqueIds: true });
  const report = await sync(api, 'owner', 1, events);
  assert.equal(report.events.length, 19);
  assert.equal(new Set(report.events.map(change => change.item_id)).size, 19);
  assert.equal(requests.length, 9);
  for (const request of requests.filter(request => request.query.includes('mutation Batch'))) {
    assert.ok(Object.keys(request.variables).length <= 20);
    for (const key of Object.keys(request.variables))
      assert.ok(request.query.includes(`$${key}:`));
  }
});

test('an unchanged feed needs only three reads and retains its report', async () => {
  const current = item(prepared.id, 'Done');
  current.content = { ...current.content, title: prepared.summary, body: prepared.body };
  current.start = { date: dateOnly(event.dtstart) };
  current.end = { date: dateOnly(event.dtend) };
  current.location = { text: event.location };
  const { api, calls, requests } = fixture([current]);
  const report = await sync(api, 'owner', 1, [prepared]);
  assert.equal(requests.length, 3);
  assert.ok(!calls.some(call => call.input));
  assert.equal(report.events[0].new_status, 'Done');
});

test('a changed end date writes only that field', async () => {
  const current = item(prepared.id, 'Done');
  current.content = { ...current.content, title: prepared.summary, body: prepared.body };
  current.start = { date: dateOnly(event.dtstart) };
  current.end = { date: '2026-10-21' };
  current.location = { text: event.location };
  const { api, calls, requests } = fixture([current]);
  await sync(api, 'owner', 1, [prepared]);
  const writes = calls.filter(call => call.input);
  assert.equal(requests.length, 4);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].input.fieldId, 'End');
  assert.deepEqual(writes[0].input.value, { date: '2026-10-20' });
});

test('creation batches retain the correct item IDs across batch boundaries', async () => {
  const events = loadEvents(Array.from({ length: 21 }, (_, index) =>
    ({ source: { ...event, summary: `Meet ${index}` } })));
  const { api, calls } = fixture([], { uniqueIds: true });
  const report = await sync(api, 'owner', 1, events);
  const ids = calls.filter(call => call.operation === 'Set' && call.input.fieldId === 'ID');
  assert.equal(new Set(report.events.map(change => change.item_id)).size, 21);
  assert.deepEqual(ids.map(call => [call.input.itemId, call.input.value.text]),
    report.events.map(change => [change.item_id, change.event_id]));
});

test('a failed batch stops the sync instead of reporting partial success', async () => {
  const { api } = fixture();
  let writes = 0;
  const failing = (query, variables) => {
    if (query.includes('mutation Batch') && ++writes === 2)
      throw new Error('partial GraphQL failure');
    return api(query, variables);
  };
  await assert.rejects(sync(failing, 'owner', 1, [prepared]), /partial GraphQL failure/);
});

test('deletes expired managed drafts of every status, preferring End over Start', async () => {
  const cases = [
    ['old-todo', 'Todo', '2026-09-01', '2026-10-06'],
    ['old-done', 'Done', '2026-09-01', '2026-10-07'],
    ['old-old', 'Old', '2026-10-07', null],
    ['recent-end', 'Done', '2026-09-01', '2026-10-08'],
    ['recent-start', 'Done', '2026-10-08', null],
    ['missing-dates', 'Done', null, null],
    ['invalid-end', 'Done', '2026-10-07', 'invalid'],
  ];
  const items = cases.map(([summary, status, start, end]) => ({
    ...item(eventId({ ...event, summary }), status, summary),
    start: start ? { date: start } : null,
    end: end ? { date: end } : null,
  }));
  const { api, calls, requests } = fixture(items, { paginate: true });
  assert.deepEqual(await sync(api, 'owner', 1, []), { events: [] });
  assert.deepEqual(calls.filter(call => call.operation === 'Delete').map(call => call.input),
    ['old-todo', 'old-done', 'old-old', 'invalid-end'].map(itemId =>
      ({ projectId: 'project', itemId })));
  assert.ok(!calls.some(call => ['Set', 'Refresh', 'Create'].includes(call.operation)));
  const deletion = requests.find(request => request.query.includes('deleteProjectV2Item'));
  assert.match(deletion.query, /DeleteProjectV2ItemInput!/);
  assert.match(deletion.query, /deletedItemId/);
});

test('expired feed entries are deleted without being recreated, refreshed or reported', async () => {
  const events = loadEvents([
    { boundary: { ...event, summary: 'Exactly two weeks',
      dtend: '2026-10-07T14:00:00+02:00' } },
    { older: { ...event, summary: 'Older', dtend: '2026-10-07T11:59:59Z' } },
    { missing: { ...event, summary: 'No end', dtstart: '2026-10-07T12:00:00Z', dtend: null } },
    { invalid: { ...event, summary: 'Invalid end',
      dtstart: '2026-10-07T12:00:00Z', dtend: 'invalid' } },
    { recent: { ...event, summary: 'Just under two weeks',
      dtend: '2026-10-07T14:00:01+02:00' } },
    { ongoing: { ...event, summary: 'Ongoing', dtstart: '2026-09-01',
      dtend: '2026-10-22' } },
  ]);
  const { api, calls } = fixture(events.slice(0, 4).map(value => ({
    ...item(value.id, 'Todo', value.summary),
    start: { date: dateOnly(value.dtstart) },
    end: { date: dateOnly(value.dtend) },
  })), { uniqueIds: true });
  const report = await sync(api, 'owner', 1, events);
  assert.equal(calls.filter(call => call.operation === 'Delete').length, 4);
  assert.equal(calls.filter(call => call.operation === 'Create').length, 2);
  assert.ok(!calls.some(call => call.operation === 'Refresh'));
  assert.deepEqual(report.events.map(value => value.event_id), events.slice(4).map(value => value.id));
});

test('expired board items are deleted and recreated from future feed events in one batch',
  async () => {
  const current = item(prepared.id, 'Done');
  current.start = { date: '2026-10-19' };
  current.end = { date: '2026-10-01' };
  const { api, calls, requests } = fixture([current]);
  const report = await sync(api, 'owner', 1, [prepared]);
  assert.deepEqual(calls.filter(call => call.input).slice(0, 2).map(call => call.operation),
    ['Delete', 'Create']);
  assert.ok(!calls.some(call => call.operation === 'Refresh'));
  const batch = requests.find(request => request.query.includes('mutation Batch'));
  assert.match(batch.query, /deleteProjectV2Item/);
  assert.match(batch.query, /addProjectV2DraftIssue/);
  assert.equal(report.events[0].item_id, 'created');
  assert.equal(report.events[0].is_new, true);
});

test('mixed deletion and creation batches retain new item IDs across batch boundaries', async () => {
  const stale = Array.from({ length: 21 }, (_, index) => ({
    ...item(eventId({ ...event, summary: `Stale ${index}` }), 'Done', `stale-${index}`),
    end: { date: '2026-09-01' },
  }));
  const events = loadEvents(Array.from({ length: 21 }, (_, index) =>
    ({ source: { ...event, summary: `Future ${index}` } })));
  const { api, calls, requests } = fixture(stale, { uniqueIds: true });
  const report = await sync(api, 'owner', 1, events);
  assert.equal(calls.filter(call => call.operation === 'Delete').length, 21);
  const creations = calls.filter(call => call.operation === 'Create');
  assert.equal(creations.length, 21);
  assert.deepEqual(report.events.map(change => change.item_id),
    creations.map(call => `created-${calls.indexOf(call) + 1}`));
  const ids = calls.filter(call => call.operation === 'Set' && call.input.fieldId === 'ID');
  assert.deepEqual(ids.map(call => [call.input.itemId, call.input.value.text]),
    report.events.map(change => [change.item_id, change.event_id]));
  const batches = requests.filter(request => request.query.includes('mutation Batch'));
  assert.match(batches[1].query, /deleteProjectV2Item/);
  assert.match(batches[1].query, /addProjectV2DraftIssue/);
  assert.ok(batches.every(request => Object.keys(request.variables).length <= 20));
});

test('cleanup leaves archived drafts and unrelated items untouched', async () => {
  const archived = { ...item(prepared.id, 'Done', 'archived'), isArchived: true };
  const unrelated = item('manual-id', 'Todo');
  const issue = item(prepared.id, 'Todo', 'issue');
  issue.content.__typename = 'Issue';
  for (const value of [archived, unrelated, issue])
    value.end = { date: '2026-09-01' };
  const { api, calls } = fixture([archived, unrelated, issue]);
  await sync(api, 'owner', 1, []);
  assert.ok(!calls.some(call => call.input));
});

test('cleanup failure stops sync before creating or reporting events', async () => {
  const stale = item(eventId({ ...event, summary: 'Stale' }), 'Done');
  stale.end = { date: '2026-09-01' };
  const { api, calls } = fixture([stale]);
  const failing = (query, variables) => {
    if (query.includes('deleteProjectV2Item'))
      throw new Error('cleanup failed');
    return api(query, variables);
  };
  await assert.rejects(sync(failing, 'owner', 1, [prepared]), /cleanup failed/);
  assert.ok(!calls.some(call => call.input));
});