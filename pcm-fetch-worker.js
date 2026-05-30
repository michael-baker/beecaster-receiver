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
 *   { type: 'reset' }                    — stream reconnecting; clear accumulator + preroll
 *   { type: 'resetPreroll' }             — re-arm preroll after underrun (keep accumulator)
 *   { type: 'stop' }                     — terminate (worker.terminate() called after)
 *   { type: 'setDirectPort', port }      — MessageChannel port wired to AudioWorklet
 *   { type: 'pool', left, right }        — fallback pool recycle (used before direct port)
 *
 * Messages OUT to main thread:
 *   { type: 'prerollComplete' }  — PCM_PREROLL_SAMPLES have been decoded; main thread sends 'start' to worklet
 *   { type: 'syncLost' }         — frame magic not found; scanning
 *
 * Messages OUT via directPort (to AudioWorklet, bypassing main thread):
 *   { type: 'pcm', left: Float32Array, right: Float32Array, audioTimeMs: number }
 *
 * Messages IN via directPort (from AudioWorklet, bypassing main thread):
 *   { type: 'free', left: Float32Array, right: Float32Array } — pool recycle
 */
'use strict';

const PCM_MAGIC          = 0x50434DFF; // 'P','C','M',0xFF
const PCM_HDR_SIZE       = 16;
const PCM_PREROLL_SAMPLES = 240000;    // 5 s at 48 kHz — must match receiver.html

var _accum    = new Uint8Array(512 * 1024);
var _accumLen = 0;

// Float32Array buffer pool — buffers are transferred to the AudioWorklet, which copies
// them into the ring and immediately transfers them back via the direct port as 'free'.
var _pool    = [];
var _poolMax = 8;

// Pre-roll tracking (moved from main thread to avoid extra round-trips)
var _prerollSamples  = 0;
var _prerollComplete = false;

// Direct MessageChannel port to the AudioWorklet.
// When set, decoded frames are sent here instead of via self.postMessage,
// completely bypassing the main thread for the hot pcm/free traffic.
var _directPort = null;

self.onmessage = function (e) {
    var msg = e.data;
    if (msg.type === 'chunk') {
        _consume(msg.data);
    } else if (msg.type === 'reset') {
        _accumLen        = 0;
        _prerollSamples  = 0;
        _prerollComplete = false;
    } else if (msg.type === 'resetPreroll') {
        // Re-arm preroll after an underrun — keep accumulator intact
        _prerollSamples  = 0;
        _prerollComplete = false;
    } else if (msg.type === 'setDirectPort') {
        // Wire up the direct Worker↔AudioWorklet MessageChannel.
        // From this point, decoded pcm goes via port instead of self.postMessage,
        // and pool recycling (free) arrives here without touching the main thread.
        _directPort = msg.port;
        _directPort.onmessage = function (ev) {
            var m = ev.data;
            if (m.type === 'free' && _pool.length < _poolMax) {
                _pool.push({ left: m.left, right: m.right });
            }
        };
    } else if (msg.type === 'pool') {
        // Fallback: main-thread relay (only used before direct port is established)
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

        // Send decoded frame — via direct channel to AudioWorklet when available,
        // otherwise via main thread (fallback before direct port is established).
        var sendPort = _directPort || self;
        sendPort.postMessage(
            { type: 'pcm', left: left, right: right, audioTimeMs: audioTimeMs },
            [left.buffer, right.buffer]
        );

        // Pre-roll tracking: signal the main thread once enough samples are buffered.
        if (!_prerollComplete) {
            _prerollSamples += sampleCount;
            if (_prerollSamples >= PCM_PREROLL_SAMPLES) {
                _prerollComplete = true;
                self.postMessage({ type: 'prerollComplete' });
            }
        }

        pos += frameBytes;
    }

    if (pos > 0) {
        _accumLen -= pos;
        if (_accumLen > 0) _accum.copyWithin(0, pos, pos + _accumLen);
    }
}
