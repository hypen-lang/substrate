import { createConnection } from "node:net";
import { connect as connectTls } from "node:tls";

export class SubstrateError extends Error {
  constructor(code, message) { super(message); this.name = "SubstrateError"; this.code = code; }
}
export function link(alias) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) throw new Error("invalid batch alias");
  return `$${alias}`;
}
export function insertOp(descriptor, value, alias) {
  return { op: "insert", relation: descriptor.relation, schema_id: descriptor.schemaId, fingerprint: descriptor.fingerprint, fields: descriptor.encode(value), alias: alias ?? null };
}
export class LocalClient {
  #socket; #token; #capability; #connection;
  constructor(socket, token) { this.#socket = socket; this.#token = token; }
  withCapability(id) {
    if (typeof id !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) throw new Error("expected capability UUID");
    if (this.#capability && this.#capability !== id) throw new Error("cannot switch selected capability");
    const client = new LocalClient(this.#socket, this.#token); client.#capability = id; client.#connection = this.#connection; return client;
  }
  /** One bounded sequential connection. No automatic reconnection after failure. */
  persistent() {
    const client = new LocalClient(this.#socket, this.#token);
    client.#capability = this.#capability;
    client.#connection = this.#connection ?? new PersistentConnection(this.#socket);
    return client;
  }
  close() { this.#connection?.close(); }
  // No transport retries: mutation outcome may be uncertain after a disconnect.
  async request(command) {
    if (this.#capability) command = { op: "using_capability", capability: this.#capability, command };
    const payload = JSON.stringify({ version: 1, token: this.#token, command }) + "\n";
    if (Buffer.byteLength(payload) > 1024 * 1024) throw new SubstrateError("limit", "request too large");
    if (this.#connection) return this.#connection.request(payload);
    return new Promise((resolve, reject) => {
      const remote = typeof this.#socket === "object";
      const socket = remote ? connectTls({ ...this.#socket, rejectUnauthorized: true, minVersion: "TLSv1.3", maxVersion: "TLSv1.3" }) : createConnection(this.#socket);
      const chunks = []; let length = 0; let done = false;
      const timer = setTimeout(() => finish(new SubstrateError("transport", "timeout; mutation outcome may be uncertain")), 10000);
      function finish(error, value) {
        if (done) return; done = true; clearTimeout(timer); socket.destroy();
        if (error) reject(error); else resolve(value);
      }
      socket.once(remote ? "secureConnect" : "connect", () => socket.write(payload));
      socket.on("error", error => finish(new SubstrateError("transport", `connection failed (${error.code ?? "I/O"}); mutation outcome may be uncertain`)));
      socket.on("end", () => { if (!done) finish(new SubstrateError("transport", "incomplete response; mutation outcome may be uncertain")); });
      socket.on("data", data => {
        chunks.push(data); length += data.length;
        if (length > 4 * 1024 * 1024) { finish(new SubstrateError("limit", "response too large")); return; }
        if (!data.includes(10)) return;
        try {
          const raw = Buffer.concat(chunks); const response = JSON.parse(raw.subarray(0, raw.indexOf(10)).toString("utf8"));
          if (response.status === "error") finish(new SubstrateError(response.value.code, response.value.message));
          else if (response.status === "ok") finish(null, response.value);
          else finish(new SubstrateError("protocol", "invalid response"));
        } catch { finish(new SubstrateError("protocol", "invalid response")); }
      });
    });
  }
  async capabilities() { return this.request({ op: "capabilities" }); }
  async batch(operations) {
    const contracts = operations.map(op => ({ relation: op.relation, schema_id: op.schema_id, fingerprint: op.fingerprint }));
    const wire = operations.map(({ fingerprint, ...op }) => op);
    const response = await this.request({ op: "checked", contracts, command: { op: "batch", operations: wire } });
    if (response.kind !== "batch") throw new SubstrateError("protocol", "expected batch result");
    return response.ids;
  }
  async insert(descriptor, value) { return (await this.batch([insertOp(descriptor, value)]))[0]; }
  async update(descriptor, id, value) {
    await this.batch([{ op: "update", relation: descriptor.relation, schema_id: descriptor.schemaId, fingerprint: descriptor.fingerprint, id, fields: descriptor.encodePatch(value) }]);
  }
  async delete(descriptor, id) { await this.batch([{ op: "delete", relation: descriptor.relation, schema_id: descriptor.schemaId, fingerprint: descriptor.fingerprint, id }]); }
  async page(descriptor, options = {}) {
    const response = await this.request({ op: "checked", contracts: [{relation: descriptor.relation, schema_id: descriptor.schemaId, fingerprint: descriptor.fingerprint}], command: { op: "page", relation: descriptor.relation, schema_id: descriptor.schemaId, after: options.after ?? null, limit: options.limit ?? 100, expected_revision: options.expectedRevision ?? null, filter: options.filter ?? null } });
    if (response.kind !== "page") throw new SubstrateError("protocol", "expected page result");
    return { revision: response.revision, next: response.next, rows: response.rows.map(row => descriptor.decode(row)) };
  }
  async queryChecked(sql, params, resultSchema) {
    const result = await this.query(sql, params);
    if (new Set(result.columns).size !== result.columns.length) throw new SubstrateError("query", "duplicate result columns; use aliases");
    const rows = [];
    for (const values of result.rows) {
      const checked = await resultSchema["~standard"].validate(Object.fromEntries(values.map((v, i) => [result.columns[i], v])));
      if (checked.issues) throw new SubstrateError("validation", checked.issues.map(i => i.message).join("; "));
      rows.push(checked.value);
    }
    return rows;
  }
  async querySelected(sql, params = [], descriptors = []) {
    const contracts = descriptors.map(descriptor => ({relation: descriptor.relation, schema_id: descriptor.schemaId, fingerprint: descriptor.fingerprint}));
    const response = await this.request({op: "checked", contracts, command: {op: "query", sql, params}});
    if (response.kind !== "query") throw new SubstrateError("protocol", "expected query result");
    return { columns: response.columns, rows: response.rows.map(row => row.map(decodeSql)) };
  }
  async query(sql, params = []) {
    const response = await this.request({ op: "query", sql, params });
    if (response.kind !== "query") throw new SubstrateError("protocol", "expected query result");
    return { columns: response.columns, rows: response.rows.map(row => row.map(decodeSql)) };
  }
}
function decodeSql(value) {
  if (value === "Null") return null;
  if ("Integer" in value) return BigInt(value.Integer);
  if ("Text" in value) return value.Text;
  if ("Real" in value) return value.Real;
  if ("Blob" in value) return Uint8Array.from(value.Blob);
  throw new SubstrateError("protocol", "invalid SQL value");
}

/** TLS 1.3 with CA and hostname verification. Credentials already bind a capability. */
export class RemoteClient extends LocalClient {
  constructor(options, token) {
    if (!options || typeof options.host !== "string" || typeof options.servername !== "string" || !options.servername || !Number.isInteger(options.port) || options.port < 1 || options.port > 65535 || !(typeof options.ca === "string" || Buffer.isBuffer(options.ca))) throw new Error("expected TLS host, port, servername and CA");
    // Copy only supported settings; callers cannot disable certificate verification.
    super(Object.freeze({host: options.host, port: options.port, servername: options.servername, ca: typeof options.ca === "string" ? options.ca : Buffer.from(options.ca)}), token);
  }
}

// One in-flight frame gives unambiguous FIFO correlation without changing protocol v1.
// Queue bounds also bound retained serialized payloads. Transport failure is terminal.
class PersistentConnection {
  #target; #socket; #ready=false; #closed=false; #queue=[]; #active; #bytes=0; #chunks=[]; #length=0; #connectTimer;
  constructor(target) { this.#target=target; }
  request(payload) {
    if(this.#closed)return Promise.reject(new SubstrateError("closed","session closed; reconnect explicitly after reconciling uncertain mutations"));
    const size=Buffer.byteLength(payload);
    if(this.#queue.length+(this.#active?1:0)>=16||this.#bytes+size>4*1024*1024)return Promise.reject(new SubstrateError("limit","persistent request queue is full"));
    return new Promise((resolve,reject)=>{
      this.#bytes+=size;this.#queue.push({payload,size,resolve,reject});
      if(!this.#socket)this.#connect();
      this.#drain();
    });
  }
  close() {this.#fail(new SubstrateError("closed","session closed; an in-flight mutation may have committed"));}
  #connect() {
    const remote=typeof this.#target==="object";
    this.#socket=remote?connectTls({...this.#target,rejectUnauthorized:true,minVersion:"TLSv1.3",maxVersion:"TLSv1.3"}):createConnection(this.#target);
    this.#socket.setNoDelay?.(true);
    this.#connectTimer=setTimeout(()=>this.#fail(new SubstrateError("transport","connection timeout")),10000);
    this.#socket.once(remote?"secureConnect":"connect",()=>{clearTimeout(this.#connectTimer);this.#ready=true;this.#drain();});
    this.#socket.on("error",error=>this.#fail(new SubstrateError("transport",`connection failed (${error.code??"I/O"}); mutation outcome may be uncertain`)));
    this.#socket.on("end",()=>this.#fail(new SubstrateError("transport","session ended; mutation outcome may be uncertain")));
    this.#socket.on("close",()=>this.#fail(new SubstrateError("transport","session closed; mutation outcome may be uncertain")));
    this.#socket.on("data",data=>this.#receive(data));
  }
  #drain() {
    if(!this.#ready||this.#closed||this.#active||!this.#queue.length)return;
    const item=this.#active=this.#queue.shift();
    item.timer=setTimeout(()=>this.#fail(new SubstrateError("transport","request timeout; mutation outcome may be uncertain")),10000);
    this.#socket.write(item.payload);
  }
  #receive(data) {
    if(!this.#active){this.#fail(new SubstrateError("protocol","unsolicited response"));return;}
    this.#chunks.push(data);this.#length+=data.length;
    if(this.#length>4*1024*1024+1){this.#fail(new SubstrateError("limit","response too large; mutation outcome may be uncertain"));return;}
    if(!data.includes(10))return;
    const bytes=Buffer.concat(this.#chunks);const end=bytes.indexOf(10);
    if(end!==bytes.length-1){this.#fail(new SubstrateError("protocol","unexpected extra response frame; mutation outcome may be uncertain"));return;}
    let response;
    try{response=JSON.parse(bytes.subarray(0,end).toString("utf8"));if(!response||!["ok","error"].includes(response.status)||response.status==="error"&&(typeof response.value?.code!=="string"||typeof response.value?.message!=="string"))throw new Error();}
    catch{this.#fail(new SubstrateError("protocol","invalid response; mutation outcome may be uncertain"));return;}
    const item=this.#active;clearTimeout(item.timer);this.#active=undefined;this.#bytes-=item.size;this.#chunks=[];this.#length=0;
    if(response.status==="error")item.reject(new SubstrateError(response.value.code,response.value.message));else item.resolve(response.value);
    this.#drain();
  }
  #fail(error) {
    if(this.#closed)return;this.#closed=true;clearTimeout(this.#connectTimer);
    this.#socket?.destroy();
    if(this.#active){clearTimeout(this.#active.timer);this.#active.reject(error);this.#active=undefined;}
    for(const item of this.#queue)item.reject(error);
    this.#queue=[];this.#bytes=0;this.#chunks=[];this.#length=0;
  }
}
