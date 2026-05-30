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

self.onmessage = function (e) {
    var msg = e.data;
    if (msg.type === 'chunk') {
        _consume(msg.data);
    } else if (msg.type === 'reset') {
        _accumLen = 0;
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
        var view = new DataView(_accum.buffer, _accum.byteOffset + pos, _accumLen - pos);
        var magic = view.getUint32(0, false);

        if (magic !== PCM_MAGIC) {
            self.postMessage({ type: 'syncLost' });
            pos++;
            continue;
        }

        var audioTimeMs = view.getUint32(4, false);
        var sampleCount = view.getUint32(8, false);

        if (sampleCount === 0) {
            // Close frame — server signalled end of stream; accumulator stays intact.
            pos += PCM_HDR_SIZE;
            continue;
        }

        var frameBytes = PCM_HDR_SIZE + sampleCount * 4; // 2 ch × 2 bytes
        if (pos + frameBytes > _accumLen) break; // incomplete frame — wait for more data

        var pcmBytes = new Uint8Array(_accum.buffer, _accum.byteOffset + pos + PCM_HDR_SIZE, sampleCount * 4);
        var pcm16    = new Int16Array(pcmBytes.buffer, pcmBytes.byteOffset, sampleCount * 2);
        var left     = new Float32Array(sampleCount);
        var right    = new Float32Array(sampleCount);
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
