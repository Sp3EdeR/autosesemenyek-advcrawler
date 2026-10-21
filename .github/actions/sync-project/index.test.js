const { test } = require('node:test');
const assert = require('node:assert/strict');
const { dateOnly, eventId, markdown, loadEvents, createClient, sync } = require('./index');

const event = { summary: 'Car Meet', dtstart: '2026-10-19T00:30:00+02:00',
  dtend: '2026-10-20T18:00:00+02:00', location: 'Budapest',
  url: 'https://example.com/event', description: '<b>Welcome</b>', geo: '47.5;19.0' };
const prepared = loadEvents([{ source: event }])[0];

/** Mock the project API at its operation boundary and retain every mutation. */
function fixture(items = [], options = {}) {
  const calls = [];
  const fields = ['Start', 'End', 'Location', 'ID', 'Status'].map(name => ({
    id: name, name, dataType: ['Start', 'End'].includes(name) ? 'DATE'
      : name === 'Status' ? 'SINGLE_SELECT' : 'TEXT',
    options: ['Todo', 'Old', 'Done'].map(value => ({ id: value, name: value })),
  }));
  const api = async (query, variables) => {
    const operation = query.match(/(?:query|mutation) (\w+)/)[1];
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
  return { api, calls };
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