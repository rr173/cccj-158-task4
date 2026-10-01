import fs from 'node:fs';
import path from 'node:path';
import { crc32 } from './crypto-util.mjs';

/**
 * Append-only write-ahead log.
 *
 * Frame layout on disk:
 *   [magic 2B = 0x43 0x56][type 1B][length 4B BE][payload length B][crc32 4B BE over type+length+payload]
 *
 * Recovery semantics:
 *  - a short final frame (process died mid write) is truncated away
 *  - a complete final frame with a bad CRC is truncated away (torn write)
 *  - a bad frame anywhere before a valid frame is fatal: refusing to continue
 *    avoids silently skipping a committed event.
 */

const MAGIC = Buffer.from([0x43, 0x56]);
const HEADER_LEN = 2 + 1 + 4;
const CRC_LEN = 4;
const MAX_PAYLOAD = 8 * 1024 * 1024;

export class WalError extends Error {}

export class Wal {
  constructor(filePath) {
    this.filePath = filePath;
    this.fd = null;
    this.fsyncOnAppend = true;
  }

  open() {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    this.fd = fs.openSync(this.filePath, 'a+');
  }

  append(type, payload) {
    const body = Buffer.from(payload);
    if (body.length > MAX_PAYLOAD) throw new WalError('frame too large');
    const typeBuf = Buffer.from([type & 0xff]);
    const lenBuf = Buffer.alloc(4);
    lenBuf.writeUInt32BE(body.length, 0);
    const crcInput = Buffer.concat([typeBuf, lenBuf, body]);
    const crcBuf = Buffer.alloc(4);
    crcBuf.writeUInt32BE(crc32(crcInput), 0);
    const frame = Buffer.concat([MAGIC, crcInput, crcBuf]);
    if (!this.fd) throw new WalError('wal not open');
    fs.writeSync(this.fd, frame, 0, frame.length);
    fs.fsyncSync(this.fd);
  }

  close() {
    if (this.fd) {
      try {
        fs.fsyncSync(this.fd);
      } catch {
        /* ignore on close */
      }
      fs.closeSync(this.fd);
      this.fd = null;
    }
  }

  /** Read all frames. Returns { events: [{type, payload(Buffer)}], truncatedBytes }. */
  readAll() {
    let buf;
    try {
      buf = fs.readFileSync(this.filePath);
    } catch (e) {
      if (e.code === 'ENOENT') return { events: [], truncatedBytes: 0 };
      throw e;
    }

    const events = [];
    let off = 0;
    let truncatedBytes = 0;

    while (off < buf.length) {
      const frameStart = off;

      // Need at least the header.
      if (buf.length - off < HEADER_LEN) {
        truncatedBytes = buf.length - frameStart;
        break;
      }
      if (buf[off] !== MAGIC[0] || buf[off + 1] !== MAGIC[1]) {
        throw new WalError(
          `bad magic at offset ${off}${events.length ? ' (corruption before committed frames)' : ''}`,
        );
      }
      const type = buf[off + 2];
      const length = buf.readUInt32BE(off + 3, 4);
      if (length > MAX_PAYLOAD) {
        throw new WalError(`frame length implausible at offset ${off}`);
      }
      const end = off + HEADER_LEN + length + CRC_LEN;
      if (end > buf.length) {
        // Torn tail: header claimed more bytes than exist.
        truncatedBytes = buf.length - frameStart;
        break;
      }
      const wantCrc = buf.readUInt32BE(end - CRC_LEN, 4);
      const gotCrc = crc32(buf.subarray(off + 2, end - CRC_LEN));
      if (wantCrc !== gotCrc) {
        // Complete frame with bad CRC: only tolerable if it is the last frame.
        if (end === buf.length) {
          truncatedBytes = end - frameStart;
          break;
        }
        throw new WalError(`crc mismatch at offset ${off}; later frames exist, refusing to skip`);
      }
      events.push({ type, payload: buf.subarray(off + HEADER_LEN, end - CRC_LEN) });
      off = end;
    }

    return { events, truncatedBytes };
  }

  /** Drop the tail (used to remove a torn frame before appending). */
  truncate(newLength) {
    fs.truncateSync(this.filePath, newLength);
    if (this.fd) {
      fs.closeSync(this.fd);
      this.fd = fs.openSync(this.filePath, 'a+');
    }
  }
}
