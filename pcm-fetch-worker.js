/**
 * pcm-fetch-worker.js — Web Worker for pcm_stream mode.
 *
 * Runs the /cast/pcmlive HTTP fetch and frame parsing entirely off the main
 * thread so that video compositor activity on the Chromecast cannot delay PCM
 * delivery to the AudioWorklet.
 *
 * Messages IN from main thread:
 *   { type: 'start', url: 'http://…/cast/pcmlive' }  — begin fetching
 *   { type: 'stop' }                                   — abort and idle
 *
 * Messages OUT to main thread:
 *   { type: 'pcm',  left: Float32Array, right: Float32Array, audioTimeMs: number }
 *   { type: 'ended' }           — server closed the stream; worker will reconnect
 *   { type: 'error', status }   — HTTP error status
 *   { type: 'fetchError', message } — network / abort error
 *   { type: 'syncLost' }        — frame magic not found; scanning
 */
'use strict';

const PCM_MAGIC    = 0x50434DFF; // 'P','C','M',0xFF
const PCM_HDR_SIZE = 16;

var _ctrl    = null; // AbortController for the active fetch
var _accum   = new Uint8Array(512 * 1024);
var _accumLen = 0;

self.onmessage = function (e) {
    var msg = e.data;
    if (msg.type === 'start') {
        _fetchLoop(msg.url);
    } else if (msg.type === 'stop') {
        if (_ctrl) { _ctrl.abort(); _ctrl = null; }
        _accumLen = 0;
    }
};

function _fetchLoop(url) {
    if (_ctrl) _ctrl.abort();
    _ctrl = new AbortController();
    _accumLen = 0;

    fetch(url, { signal: _ctrl.signal })
        .then(function (resp) {
            if (!resp.ok) {
                self.postMessage({ type: 'error', status: resp.status });
                setTimeout(function () { _fetchLoop(url); }, 2000);
                return;
            }
            var reader = resp.body.getReader();
            function pump() {
                return reader.read().then(function (result) {
                    if (result.done) {
                        self.postMessage({ type: 'ended' });
                        setTimeout(function () { _fetchLoop(url); }, 1000);
                        return;
                    }
                    _consume(result.value);
                    return pump();
                });
            }
            return pump();
        })
        .catch(function (err) {
            if (err && err.name === 'AbortError') return;
            self.postMessage({ type: 'fetchError', message: String(err) });
            setTimeout(function () { _fetchLoop(url); }, 2000);
        });
}

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
            // Scan one byte at a time until we find the magic.
            self.postMessage({ type: 'syncLost' });
            pos++;
            continue;
        }

        var audioTimeMs = view.getUint32(4, false);
        var sampleCount = view.getUint32(8, false);

        if (sampleCount === 0) {
            // Close frame — server signalled end of stream.
            pos += PCM_HDR_SIZE;
            continue;
        }

        var frameBytes = PCM_HDR_SIZE + sampleCount * 4; // 2 ch × 2 bytes
        if (pos + frameBytes > _accumLen) break; // incomplete frame — wait for more data

        // Decode s16le interleaved → split Float32 L/R (zero-copy transfer to main thread)
        var pcmBytes = new Uint8Array(_accum.buffer, _accum.byteOffset + pos + PCM_HDR_SIZE, sampleCount * 4);
        var pcm16 = new Int16Array(pcmBytes.buffer, pcmBytes.byteOffset, sampleCount * 2);
        var left  = new Float32Array(sampleCount);
        var right = new Float32Array(sampleCount);
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
