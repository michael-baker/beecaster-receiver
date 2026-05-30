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
 *   - Messages TO decode Worker (directPort): { type: 'free', lefts, rights } — batch pool recycle
 *
 * Performance notes:
 *   - Incoming pcm messages are queued in _pendingFrames (O(1) push, no copy, no postMessage).
 *     process() drains the queue: ring copies via TypedArray.set(), then one batched 'free'
 *     postMessage per process() call — eliminates the burst-of-messages problem where the
 *     audio render thread was blocked up to 82ms draining a queue of 10+ frames.
 *   - _clockMsg is pre-allocated and mutated in-place (no per-call heap allocation).
 *   - Ring reads/writes use TypedArray.set() with explicit wrap handling.
 *   - CLOCK_INTERVAL = 4096 (~85 ms) — sufficient for A/V rate steering.
 */
'use strict';

const RING_FRAMES    = 480000; // 10 s ring buffer at 48 kHz
const FADE_SAMPLES   = 240;    // 5 ms fade on underrun/start to avoid clicks
const CLOCK_INTERVAL = 4096;   // post clock every ~85 ms

class PcmSinkProcessor extends AudioWorkletProcessor {
    constructor() {
        super();

        // Ring buffer — separate L/R so process() can bulk-copy into outputs
        this._ringL = new Float32Array(RING_FRAMES);
        this._ringR = new Float32Array(RING_FRAMES);
        this._writeHead = 0;
        this._readHead  = 0;

        // Underrun / fade state
        this._underrun        = false;
        this._fadeSamplesLeft = 0;

        // Running sample counter for clock messages
        this._samplesPlayed      = 0;
        this._lastClockSamples   = 0;

        // Clock anchor: timestamp of the last received frame + samples played at that point
        this._lastFrameAudioTimeMs     = 0;
        this._samplesPlayedAtLastFrame = 0;

        // Gate: set true when main thread sends { type: 'start' }
        this._started = false;

        // Direct MessageChannel port to the decode Worker (set via 'setDirectPort').
        this._directPort = null;

        // Incoming-frame queue: _handlePcm pushes here (O(1), no copy).
        // _drainPending() in process() consumes it: ring copy + batch free postMessage.
        this._pendingFrames = [];

        // Pre-allocated clock message — mutated in-place each time it's sent.
        this._clockMsg = {
            type: 'clock',
            audioTimeMs: 0,
            samplesPlayedAtFrame: 0,
            samplesPlayed: 0,
            ringFill: 0,
            ringCapacity: RING_FRAMES,
        };

        // Reusable arrays for the batch free postMessage built in _drainPending().
        // These are cleared and refilled each call; no new arrays are allocated.
        this._freeLefts     = [];
        this._freeRights    = [];
        this._freeTransfers = [];

        this.port.onmessage = (e) => {
            const msg = e.data;
            if (msg.type === 'pcm') {
                // Fallback: pcm arrived via main-thread port (before direct port is wired).
                // Should not happen in normal operation but handled for safety.
                this._pendingFrames.push(msg);
            } else if (msg.type === 'setDirectPort') {
                // Wire up the direct Worker↔Worklet channel.
                this._directPort = msg.port;
                this._directPort.onmessage = (ev) => {
                    // Ultra-fast: just enqueue the frame reference — no copy, no postMessage.
                    // process() will drain the queue and bulk-free all buffers in one shot.
                    if (ev.data.type === 'pcm') this._pendingFrames.push(ev.data);
                };
            } else if (msg.type === 'start') {
                this._started         = true;
                this._underrun        = false;
                this._fadeSamplesLeft = FADE_SAMPLES;
            } else if (msg.type === 'reset') {
                this._writeHead        = 0;
                this._readHead         = 0;
                this._started          = false;
                this._underrun         = false;
                this._fadeSamplesLeft  = 0;
                this._samplesPlayed    = 0;
                this._lastClockSamples = 0;
                this._pendingFrames.length = 0;
            }
        };
    }

    // Drain all queued pcm frames into the ring, then send ONE batched 'free' message
    // back to the Worker.  Called at the start of every process() tick.
    //
    // Sending one postMessage per process() (instead of one per frame in onmessage)
    // eliminates the burst where 10+ rapid-fire messages blocked the render thread
    // for up to 82ms.  In steady state (~10ms frames, ~2.67ms process() period) the
    // queue has 0–1 entries; no postMessage is sent when the queue is empty.
    _drainPending() {
        const n = this._pendingFrames.length;
        if (n === 0) return;

        const lefts     = this._freeLefts;
        const rights    = this._freeRights;
        const transfers = this._freeTransfers;
        lefts.length = rights.length = transfers.length = 0;

        for (let i = 0; i < n; i++) {
            const f = this._pendingFrames[i];
            this._pushChunk(f.left, f.right, f.audioTimeMs);
            lefts.push(f.left);
            rights.push(f.right);
            transfers.push(f.left.buffer, f.right.buffer);
        }
        this._pendingFrames.length = 0;

        // Return all consumed buffers to the Worker pool in a single message.
        const replyPort = this._directPort || this.port;
        replyPort.postMessage({ type: 'free', lefts, rights }, transfers);
    }

    // Bulk-copy left/right chunks into the ring using TypedArray.set() with
    // explicit wrap handling — avoids per-sample JS and per-iteration modulo.
    _pushChunk(left, right, audioTimeMs) {
        const n    = left.length;
        const fill = (this._writeHead - this._readHead + RING_FRAMES) % RING_FRAMES;
        if (n > RING_FRAMES - fill) {
            // Ring full — discard oldest samples to make room.
            this._readHead = (this._readHead + (n - (RING_FRAMES - fill)) + 256) % RING_FRAMES;
        }

        const toEnd = RING_FRAMES - this._writeHead;
        if (toEnd >= n) {
            this._ringL.set(left,  this._writeHead);
            this._ringR.set(right, this._writeHead);
            this._writeHead += n;
            if (this._writeHead >= RING_FRAMES) this._writeHead = 0;
        } else {
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

        // Drain any queued frames into the ring (and batch-free their buffers) before
        // outputting.  This is the only place ring writes happen, ensuring they never
        // compete with onmessage for execution time on the render thread.
        this._drainPending();

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
            // Fade-in: per-sample gain ramp (only active for FADE_SAMPLES ≈ 2 callbacks)
            for (let i = 0; i < blockSize; i++) {
                const g = 1.0 - (this._fadeSamplesLeft / FADE_SAMPLES);
                if (this._fadeSamplesLeft > 0) this._fadeSamplesLeft--;
                outL[i] = this._ringL[this._readHead] * g;
                outR[i] = this._ringR[this._readHead] * g;
                this._readHead = (this._readHead + 1) % RING_FRAMES;
            }
        } else {
            // Fast path: bulk TypedArray copy
            const toEnd = RING_FRAMES - this._readHead;
            if (toEnd >= blockSize) {
                outL.set(this._ringL.subarray(this._readHead, this._readHead + blockSize));
                outR.set(this._ringR.subarray(this._readHead, this._readHead + blockSize));
                this._readHead += blockSize;
                if (this._readHead >= RING_FRAMES) this._readHead = 0;
            } else {
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
