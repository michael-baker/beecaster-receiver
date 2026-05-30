/**
 * pcm-fetch-worker.js — Web Worker for pcm_stream mode.
 *
 * Performs frame parsing and Int16→Float32 decode off the main thread.
 * The main thread does the HTTP fetch (so mixed-content rules apply in the
 * page context, not the worker context) and forwards raw byte chunks here
 * via postMessage with transferable ArrayBuffers.
 *
 * Messages IN from main thread:
 *   { type: 'chunk', data: Uint8Array }  — raw bytes from /cast/pcmlive
 *   { type: 'reset' }                    — stream reconnecting; clear accumulator
 *   { type: 'stop' }                     — terminate (worker.terminate() called after)
 *   { type: 'pool',  left: Float32Array, right: Float32Array }  — recycled buffers from AudioWorklet
 *
 * Messages OUT to main thread:
 *   { type: 'pcm', left: Float32Array, right: Float32Array, audioTimeMs: number }
 *   { type: 'syncLost' }   — frame magic not found; scanning
 */
'use strict';

const PCM_MAGIC    = 0x50434DFF; // 'P','C','M',0xFF
const PCM_HDR_SIZE = 16;

var _accum    = new Uint8Array(512 * 1024);
var _accumLen = 0;

// Float32Array buffer pool — buffers are transferred to the AudioWorklet, which copies
// them into the ring and immediately transfers them back via the main thread as 'pool'
// messages.  Steady-state allocation rate drops to near zero (~30 allocs/s → 0).
var _pool    = [];
var _poolMax = 8;

self.onmessage = function (e) {
    var msg = e.data;
    if (msg.type === 'chunk') {
        _consume(msg.data);
    } else if (msg.type === 'reset') {
        _accumLen = 0;
    } else if (msg.type === 'pool') {
        // Receive freed Float32Array pair transferred back from AudioWorklet via main thread.
        if (_pool.length < _poolMax) _pool.push({ left: msg.left, right: msg.right });
    }
};

function _consume(chunk) {
    var needed = _accumLen + chunk.length;
    if (needed > _accum.length) {
        var bigger = new Uint8Array(Math.max(needed, _accum.length * 2));
        bigger.set(_accum.subarray(0, _accumLen));
        _accum = bigger;
    }
    _accum.set(chunk, _accumLen);
    _accumLen += chunk.length;

    var pos = 0;
    while (pos + PCM_HDR_SIZE <= _accumLen) {
        // Parse big-endian header fields directly — avoids allocating a DataView per frame.
        var magic = ((_accum[pos] << 24) | (_accum[pos+1] << 16) | (_accum[pos+2] << 8) | _accum[pos+3]) >>> 0;

        if (magic !== PCM_MAGIC) {
            self.postMessage({ type: 'syncLost' });
            pos++;
            continue;
        }

        var audioTimeMs = ((_accum[pos+4] << 24) | (_accum[pos+5] << 16) | (_accum[pos+6] << 8) | _accum[pos+7]) >>> 0;
        var sampleCount = ((_accum[pos+8] << 24) | (_accum[pos+9] << 16) | (_accum[pos+10] << 8) | _accum[pos+11]) >>> 0;

        if (sampleCount === 0) {
            // Close frame — server signalled end of stream; accumulator stays intact.
            pos += PCM_HDR_SIZE;
            continue;
        }

        var frameBytes = PCM_HDR_SIZE + sampleCount * 4; // 2 ch × 2 bytes
        if (pos + frameBytes > _accumLen) break; // incomplete frame — wait for more data

        // Direct Int16Array view into the accumulator — skips an intermediate Uint8Array allocation.
        var pcm16 = new Int16Array(_accum.buffer, _accum.byteOffset + pos + PCM_HDR_SIZE, sampleCount * 2);
        // Reuse a pooled Float32Array pair when available; otherwise allocate a fresh one.
        var pair  = null;
        for (var pi = 0; pi < _pool.length; pi++) {
            if (_pool[pi].left.length === sampleCount) { pair = _pool.splice(pi, 1)[0]; break; }
        }
        var left  = pair ? pair.left  : new Float32Array(sampleCount);
        var right = pair ? pair.right : new Float32Array(sampleCount);
        for (var i = 0; i < sampleCount; i++) {
            left[i]  = pcm16[i * 2]     / 32768.0;
            right[i] = pcm16[i * 2 + 1] / 32768.0;
        }

        self.postMessage(
            { type: 'pcm', left: left, right: right, audioTimeMs: audioTimeMs },
            [left.buffer, right.buffer]
        );

        pos += frameBytes;
    }

    if (pos > 0) {
        _accumLen -= pos;
        if (_accumLen > 0) _accum.copyWithin(0, pos, pos + _accumLen);
    }
}
