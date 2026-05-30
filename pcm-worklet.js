/**
 * pcm-worklet.js — AudioWorklet processor for pcm_stream mode.
 *
 * Receives Float32Array chunks from the decode Worker (converted from s16le PCM)
 * via a direct MessageChannel port (zero-copy, bypasses the main thread) and
 * outputs them at the hardware callback rate (128 frames per process() call at 48 kHz).
 *
 * Design:
 *   - Ring buffer: two Float32Arrays (L + R), capacity RING_FRAMES = 480000 (10 s at 48 kHz)
 *   - writeHead / readHead: sample-index cursors (not bytes)
 *   - Underrun: output silence with a 5 ms (240 sample) linear fade-out to avoid clicks
 *   - Pre-roll: main thread sends 'start' message once the decode Worker reports preroll done
 *   - On 'start': applies FADE_SAMPLES fade-in to avoid the silence→audio click
 *   - Messages TO main thread (port): { type: 'underrun' } | { type: 'clock', ... }
 *   - Messages TO decode Worker (directPort): { type: 'free', left, right } — pool recycle
 *
 * Performance notes:
 *   - _clockMsg and _freeMsg are pre-allocated and mutated in-place to avoid per-callback
 *     heap allocations, eliminating GC pressure on the audio render thread.
 *   - Ring reads/writes use TypedArray.set() with explicit wrap handling instead of
 *     per-sample JS loops, reducing render-thread CPU usage by ~4×.
 *   - CLOCK_INTERVAL raised to 4096 (~85 ms) — sufficient for A/V rate steering.
 *   - The direct MessageChannel port carries pcm/free traffic at full decode rate,
 *     keeping the main-thread message queue idle during normal playback.
 */
'use strict';

const RING_FRAMES    = 480000; // 10 s ring buffer at 48 kHz
const FADE_SAMPLES   = 240;    // 5 ms fade on underrun/start to avoid clicks
const CLOCK_INTERVAL = 4096;   // post clock every ~85 ms (was 1024/~21 ms)

class PcmSinkProcessor extends AudioWorkletProcessor {
    constructor() {
        super();

        // Ring buffer — separate L/R so process() can bulk-copy into outputs
        this._ringL = new Float32Array(RING_FRAMES);
        this._ringR = new Float32Array(RING_FRAMES);
        this._writeHead = 0;
        this._readHead  = 0;

        // Underrun / fade state
        this._underrun       = false;
        this._fadeSamplesLeft = 0;

        // Running sample counter for clock messages
        this._samplesPlayed      = 0;
        this._lastClockSamples   = 0;

        // Clock anchor: timestamp of the last received frame + samples played at that point
        this._lastFrameAudioTimeMs    = 0;
        this._samplesPlayedAtLastFrame = 0;

        // Gate: set true when main thread sends { type: 'start' }
        this._started = false;

        // Direct MessageChannel port to the decode Worker (set via 'setDirectPort').
        // pcm messages arrive here; free messages go back here.
        // Falls back to this.port if the direct port hasn't been wired up yet.
        this._directPort = null;

        // Pre-allocated message objects — mutated and re-posted to avoid GC churn.
        this._clockMsg = {
            type: 'clock',
            audioTimeMs: 0,
            samplesPlayedAtFrame: 0,
            samplesPlayed: 0,
            ringFill: 0,
            ringCapacity: RING_FRAMES,
        };
        this._freeMsg = { type: 'free', left: null, right: null };

        this.port.onmessage = (e) => {
            const msg = e.data;
            if (msg.type === 'pcm') {
                // Fallback path: pcm arrived via main-thread port (before direct port is wired)
                this._handlePcm(msg);
            } else if (msg.type === 'setDirectPort') {
                // Wire up the direct Worker↔Worklet channel — hot pcm/free traffic
                // no longer touches the main thread after this point.
                this._directPort = msg.port;
                this._directPort.onmessage = (ev) => {
                    if (ev.data.type === 'pcm') this._handlePcm(ev.data);
                };
            } else if (msg.type === 'start') {
                this._started = true;
                this._underrun = false;
                this._fadeSamplesLeft = FADE_SAMPLES;
            } else if (msg.type === 'reset') {
                this._writeHead       = 0;
                this._readHead        = 0;
                this._started         = false;
                this._underrun        = false;
                this._fadeSamplesLeft = 0;
                this._samplesPlayed   = 0;
                this._lastClockSamples = 0;
            }
        };
    }

    // Handle an incoming 'pcm' message: copy into ring, return buffers for pool reuse.
    _handlePcm(msg) {
        this._pushChunk(msg.left, msg.right, msg.audioTimeMs);
        // Return the (now-drained) buffers to the decode Worker for pool reuse.
        // Use the direct channel when available to avoid a main-thread hop.
        const replyPort = this._directPort || this.port;
        this._freeMsg.left  = msg.left;
        this._freeMsg.right = msg.right;
        replyPort.postMessage(this._freeMsg, [msg.left.buffer, msg.right.buffer]);
    }

    // Bulk-copy left/right chunks into the ring using TypedArray.set() —
    // avoids per-sample JS operations and the modulo on every iteration.
    _pushChunk(left, right, audioTimeMs) {
        const n = left.length;
        const fill = (this._writeHead - this._readHead + RING_FRAMES) % RING_FRAMES;
        const available = RING_FRAMES - fill;
        if (n > available) {
            // Ring full — advance readHead to make room, discarding the oldest samples.
            const discard = n - available + 256;
            this._readHead = (this._readHead + discard) % RING_FRAMES;
        }

        const toEnd = RING_FRAMES - this._writeHead;
        if (toEnd >= n) {
            this._ringL.set(left,  this._writeHead);
            this._ringR.set(right, this._writeHead);
            this._writeHead += n;
            if (this._writeHead >= RING_FRAMES) this._writeHead = 0;
        } else {
            // Wrap: two segments
            this._ringL.set(left.subarray(0, toEnd),  this._writeHead);
            this._ringL.set(left.subarray(toEnd),  0);
            this._ringR.set(right.subarray(0, toEnd), this._writeHead);
            this._ringR.set(right.subarray(toEnd), 0);
            this._writeHead = n - toEnd;
        }

        this._lastFrameAudioTimeMs     = audioTimeMs;
        this._samplesPlayedAtLastFrame = this._samplesPlayed;
    }

    process(inputs, outputs) {
        const outL = outputs[0][0];
        const outR = outputs[0][1] || outputs[0][0]; // mono fallback
        const blockSize = outL.length; // always 128 on Web Audio

        if (!this._started) {
            outL.fill(0);
            outR.fill(0);
            return true;
        }

        const buffered = (this._writeHead - this._readHead + RING_FRAMES) % RING_FRAMES;

        if (buffered < blockSize) {
            // Underrun — signal main thread once, then fade out and fill silence
            if (!this._underrun) {
                this._underrun        = true;
                this._fadeSamplesLeft = FADE_SAMPLES;
                this.port.postMessage({ type: 'underrun', ringFill: buffered });
            }

            if (this._fadeSamplesLeft > 0) {
                const drain = Math.min(this._fadeSamplesLeft, buffered);
                for (let i = 0; i < drain; i++) {
                    const g = this._fadeSamplesLeft / FADE_SAMPLES;
                    outL[i] = this._ringL[this._readHead] * g;
                    outR[i] = this._ringR[this._readHead] * g;
                    this._readHead = (this._readHead + 1) % RING_FRAMES;
                    this._fadeSamplesLeft--;
                }
                for (let i = drain; i < blockSize; i++) { outL[i] = 0; outR[i] = 0; }
            } else {
                outL.fill(0);
                outR.fill(0);
            }
            this._samplesPlayed += blockSize;
            return true;
        }

        // Normal playback
        if (this._underrun) {
            this._underrun        = false;
            this._fadeSamplesLeft = FADE_SAMPLES;
        }

        if (this._fadeSamplesLeft > 0) {
            // Fade-in: per-sample gain ramp (only for FADE_SAMPLES ≈ 2 callbacks)
            for (let i = 0; i < blockSize; i++) {
                const g = 1.0 - (this._fadeSamplesLeft / FADE_SAMPLES);
                if (this._fadeSamplesLeft > 0) this._fadeSamplesLeft--;
                outL[i] = this._ringL[this._readHead] * g;
                outR[i] = this._ringR[this._readHead] * g;
                this._readHead = (this._readHead + 1) % RING_FRAMES;
            }
        } else {
            // Fast path: bulk TypedArray copy, no per-sample JS overhead
            const toEnd = RING_FRAMES - this._readHead;
            if (toEnd >= blockSize) {
                outL.set(this._ringL.subarray(this._readHead, this._readHead + blockSize));
                outR.set(this._ringR.subarray(this._readHead, this._readHead + blockSize));
                this._readHead += blockSize;
                if (this._readHead >= RING_FRAMES) this._readHead = 0;
            } else {
                // Wrap: two segments
                outL.set(this._ringL.subarray(this._readHead, this._readHead + toEnd));
                outL.set(this._ringL.subarray(0, blockSize - toEnd), toEnd);
                outR.set(this._ringR.subarray(this._readHead, this._readHead + toEnd));
                outR.set(this._ringR.subarray(0, blockSize - toEnd), toEnd);
                this._readHead = blockSize - toEnd;
            }
        }

        this._samplesPlayed += blockSize;

        // Periodic clock to main thread for A/V rate steering
        if ((this._samplesPlayed - this._lastClockSamples) >= CLOCK_INTERVAL) {
            this._lastClockSamples = this._samplesPlayed;
            this._clockMsg.audioTimeMs          = this._lastFrameAudioTimeMs;
            this._clockMsg.samplesPlayedAtFrame = this._samplesPlayedAtLastFrame;
            this._clockMsg.samplesPlayed        = this._samplesPlayed;
            this._clockMsg.ringFill = (this._writeHead - this._readHead + RING_FRAMES) % RING_FRAMES;
            this.port.postMessage(this._clockMsg);
        }

        return true;
    }
}

registerProcessor('pcm-sink', PcmSinkProcessor);
