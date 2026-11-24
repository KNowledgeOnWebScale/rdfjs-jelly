import { Readable } from 'node:stream';
import type * as RDF from '@rdfjs/types';
import {
  DataFactory, JellyError, LogicalStreamType, Message, MessageDecoder, MessageEncoder, Parser, PhysicalStreamType, RdfStreamType, StreamParser, Writer,
  type DecoderState, type MessageQuadArray, type WriterOptions,
} from '../src';

const { blankNode, defaultGraph, literal, namedNode, quad } = DataFactory;

function end(writer: Writer): Promise<Uint8Array> {
  return new Promise((resolve, reject) => writer.end((error, output) => error ? reject(error) : resolve(output!)));
}

async function write(options: WriterOptions, messages: RDF.Quad[][]): Promise<Uint8Array> {
  const writer = new Writer({ delimited: true, ...options });
  if (options.streamType === RdfStreamType.MESSAGES) for (const message of messages) writer.addMessage(message);
  else writer.addQuads(messages.flat());
  return end(writer);
}

/** Byte offsets after each delimited frame. */
function frameEnds(bytes: Uint8Array): number[] {
  const ends: number[] = [];
  let pos = 0;
  while (pos < bytes.length) {
    let length = 0;
    let shift = 0;
    let byte: number;
    do {
      byte = bytes[pos++]!;
      length += (byte & 0x7f) * 2 ** shift;
      shift += 7;
    } while (byte & 0x80);
    pos += length;
    ends.push(pos);
  }
  return ends;
}

function key(entry: { quad: RDF.Quad; messageCounter: number }): string {
  const term = (t: RDF.Term): string => t.termType === 'Literal'
    ? `"${t.value}"@${t.language}^^${t.datatype.value}`
    : `${t.termType}:${t.value}`;
  const { subject, predicate, object, graph } = entry.quad;
  return `${entry.messageCounter} ${term(subject)} ${term(predicate)} ${term(object)} ${term(graph)}`;
}

function parse(bytes: Uint8Array, state?: DecoderState): { output: string[]; state: DecoderState | undefined } {
  const parser = new Parser({ messages: true, state });
  const output = (parser.parse(bytes) as MessageQuadArray).map(key);
  return { output, state: parser.getState() };
}

/** Cutting the stream after any frame and continuing from the (JSON) state gives what a single parse gives. */
function expectEveryCutDecodesExactly(bytes: Uint8Array): void {
  const full = parse(bytes).output;
  const ends = frameEnds(bytes);
  expect(ends.length).toBeGreaterThan(2);
  for (const cut of ends.slice(0, -1)) {
    const head = parse(bytes.subarray(0, cut));
    const state = JSON.parse(JSON.stringify(head.state)) as DecoderState;
    const tail = parse(bytes.subarray(cut), state);
    expect([...head.output, ...tail.output]).toEqual(full);
  }
}

const ex = (name: string): RDF.NamedNode => namedNode(`http://example.org/${name}`);
const other = (name: string): RDF.NamedNode => namedNode(`urn:other:${name}`);
const xsd = (name: string): RDF.NamedNode => namedNode(`http://www.w3.org/2001/XMLSchema#${name}`);

/** Statements that repeat terms, reuse identifiers, and need more names than a small table holds. */
function statements(count: number, graph: RDF.Quad_Graph = defaultGraph()): RDF.Quad[][] {
  const messages: RDF.Quad[][] = [];
  for (let i = 0; i < count; i++) {
    const subject = i % 3 === 0 ? blankNode(`b${i % 2}`) : ex(`s${i}`);
    messages.push([
      quad(subject, ex('p'), literal(`v${i}`, i % 2 ? 'en' : xsd(i % 4 ? 'string' : 'integer')), graph),
      quad(subject, ex('p'), literal(`v${i}`, 'nl'), graph),
      quad(subject, other(`q${i % 5}`), ex(`o${i}`), i % 4 ? graph : ex(`g${i % 3}`)),
      quad(subject, other(`q${i % 5}`), literal(`${i}`, xsd(i % 2 ? 'integer' : 'decimal')), graph),
    ]);
  }
  return messages;
}

const smallTables = { maxNames: 8, maxPrefixes: 4, maxDatatypes: 2 };

describe('decoder state', () => {
  it('continues a Jelly 1.1 QUADS stream after every frame', async () => {
    expectEveryCutDecodesExactly(await write({ version: 2, messageSize: 3, lookup: smallTables }, statements(20)));
  });

  it('continues a Jelly 1.1 TRIPLES stream after every frame', async () => {
    const triples = statements(12).map(message => message.map(q => quad(q.subject, q.predicate, q.object)));
    expectEveryCutDecodesExactly(await write({ version: 2, physicalType: PhysicalStreamType.TRIPLES, messageSize: 2, lookup: smallTables }, triples));
  });

  it('continues a Jelly 1.1 GRAPHS stream inside an open graph', async () => {
    const writer = new Writer({ version: 2, physicalType: PhysicalStreamType.GRAPHS, logicalType: LogicalStreamType.FLAT_QUADS, lookup: smallTables });
    for (const graph of [ex('g1'), ex('g2')]) {
      writer.addGraph(graph, statements(6).flat().map(q => quad(q.subject, q.predicate, q.object, graph)));
    }
    // The writer puts a graph in one frame: give every row a frame of its own, so that frames end inside a graph.
    const rows = new MessageDecoder().decode(await end(writer)).flatMap(frame => frame.rows);
    const bytes = new MessageEncoder().encodeAll(rows.map(row => ({ rows: [row], metadata: new Map() })));
    const full = parse(bytes).output;
    let insideGraph = 0;
    for (const cut of frameEnds(bytes).slice(0, -1)) {
      // A stream that ends inside a graph is invalid, so the state is read before the end of the input.
      const parser = new StreamParser({ messages: true });
      const head: string[] = [];
      parser.on('data', entry => head.push(key(entry as { quad: RDF.Quad; messageCounter: number })));
      parser.write(bytes.subarray(0, cut));
      await new Promise(resolve => setImmediate(resolve));
      const state = JSON.parse(JSON.stringify(parser.getState())) as DecoderState;
      if (state.graph) insideGraph++;
      expect([...head, ...parse(bytes.subarray(cut), state).output]).toEqual(full);
    }
    expect(insideGraph).toBeGreaterThan(10);
  });

  it('continues a Jelly 1.2 FLAT stream after every batch', async () => {
    expectEveryCutDecodesExactly(await write({ messageSize: 7, lookup: { ...smallTables, maxNames: 128 } }, statements(80)));
  });

  it('continues a Jelly 1.2 MESSAGES stream with message counters and scoped blank nodes', async () => {
    const bytes = await write({ streamType: RdfStreamType.MESSAGES, packMessages: false, lookup: { ...smallTables, maxNames: 128 } }, statements(80));
    expectEveryCutDecodesExactly(bytes);
    const head = parse(bytes.subarray(0, frameEnds(bytes)[3]));
    expect(head.state!.messageCounter).toBe(4);
    expect(head.state!.repeated).toBeUndefined();
  });

  it('is undefined before the stream options were read', () => {
    const parser = new Parser();
    expect(parser.getState()).toBeUndefined();
    parser.parse(new Uint8Array());
    expect(parser.getState()).toBeUndefined();
  });

  it('is JSON data, with repeated terms for Jelly 1.1', async () => {
    const bytes = await write({ version: 2 }, [[quad(ex('s'), ex('p'), literal('v', 'en'), ex('g'))]]);
    const parser = new Parser();
    parser.parse(bytes);
    const state = parser.getState()!;
    expect(JSON.parse(JSON.stringify(state))).toEqual(state);
    expect(state.repeated).toEqual([
      { termType: 'NamedNode', value: 'http://example.org/s' },
      { termType: 'NamedNode', value: 'http://example.org/p' },
      { termType: 'Literal', value: 'v', language: 'en' },
      { termType: 'NamedNode', value: 'http://example.org/g' },
    ]);
    expect(state.options.version).toBe(2);
  });

  it('rejects a state whose lookup references fall outside the declared tables', async () => {
    const parser = new Parser();
    parser.parse(await write({ version: 2 }, statements(1)));
    const state = parser.getState()!;
    expect(() => new Parser({ state: { ...state, names: { ...state.names, lastAssigned: state.options.maxNameTableSize + 1 } } }).parse(new Uint8Array()))
      .toThrow(/outside table size/);
    expect(() => new Parser({ state: { ...state, options: { ...state.options, version: 9 } } }).parse(new Uint8Array()))
      .toThrow(/Unsupported Jelly protocol version 9/);
  });

  it('gives the state of a Node stream parser and continues it in another one', async () => {
    const bytes = await write({ streamType: RdfStreamType.MESSAGES, packMessages: false }, statements(6));
    const cut = frameEnds(bytes)[2]!;
    const collect = async (parser: StreamParser, input: Uint8Array): Promise<string[]> => {
      const output: string[] = [];
      for await (const entry of Readable.from([input]).pipe(parser)) output.push(key(entry as { quad: RDF.Quad; messageCounter: number }));
      return output;
    };
    const first = new StreamParser({ messages: true });
    const head = await collect(first, bytes.subarray(0, cut));
    const second = new StreamParser({ messages: true, state: first.getState() });
    const tail = await collect(second, bytes.subarray(cut));
    expect([...head, ...tail]).toEqual(parse(bytes).output);
  });

  it('refuses a state while a frame is buffered or a message is open', async () => {
    const bytes = await write({ streamType: RdfStreamType.MESSAGES, messageSize: 2 }, [statements(3).flat()]);
    const ends = frameEnds(bytes);
    const partial = new StreamParser({ messages: true });
    partial.write(bytes.subarray(0, ends[0]! + 1));
    expect(() => partial.getState()).toThrow(JellyError);
    expect(() => partial.getState()).toThrow(/bytes are still buffered/);

    // The one message spans several batches: after the second, it is still open.
    const open = new StreamParser({ messages: true });
    open.write(bytes.subarray(0, ends[1]));
    expect(() => open.getState()).toThrow(/a message is still open/);
  });

  it('keeps message metadata out of the state', async () => {
    const writer = new Writer({ streamType: RdfStreamType.MESSAGES, delimited: true });
    writer.addMessage(new Message(0, [quad(ex('s'), ex('p'), ex('o'))], { id: new Uint8Array([1]) }));
    const parser = new Parser();
    parser.parse(await end(writer));
    expect(Object.keys(parser.getState()!).sort()).toEqual(['datatypes', 'messageCounter', 'names', 'options', 'prefixes']);
  });
});
