/**
 * pcm-worklet.js — AudioWorklet processor for pcm_stream mode.
 *
 * Receives Float32Array chunks from the main thread (converted from s16le PCM)
 * via port.postMessage with transfer, buffers them in a ring, and outputs them
 * at the hardware callback rate (128 frames per process() call at 48 kHz).
 *
 * POC note: audio is 16-bit (s16le converted to float32 on the main thread).
 * The pipeline can be upgraded to 24-bit after POC validation — the worklet
 * only needs to accept Float32 regardless of the original bit depth.
 *
 * Design:
 *   - Ring buffer: two Float32Arrays (L + R), capacity RING_FRAMES = 192000 (4 s at 48 kHz)
 *   - writeHead / readHead: sample-index cursors (not bytes)
 *   - Underrun: output silence with a 5 ms (240 sample) linear fade-out to avoid clicks
 *   - Pre-roll: main thread sends 'start' message once 96000 samples are buffered (2 s)
 *   - On 'start': applies FADE_SAMPLES fade-in to avoid the silence→audio click
 *   - Messages TO main thread: { type: 'underrun' } | { type: 'clock', audioTimeMs, samplesPlayed }
 */
'use strict';

const RING_FRAMES = 192000; // 4 s ring buffer at 48 kHz
const FADE_SAMPLES = 240;   // 5 ms fade-out on underrun to avoid clicks
const CLOCK_INTERVAL = 1024; // post clock message every ~21 ms

class PcmSinkProcessor extends AudioWorkletProcessor {
    constructor() {
        super();

        // Ring buffer — separate L/R arrays so process() can copy directly into outputs
        this._ringL = new Float32Array(RING_FRAMES);
        this._ringR = new Float32Array(RING_FRAMES);
        this._writeHead = 0;
        this._readHead = 0;

        // Underrun state
        this._underrun = false;
        this._fadeSamplesLeft = 0;

        // Running sample count for clock messages to main thread
        this._samplesPlayed = 0;
        this._lastClockSamples = 0;

        // The audioTimeMs of the last frame header received (for clock sync reporting)
        this._lastFrameAudioTimeMs = 0;
        // How many samples were in the ring when _lastFrameAudioTimeMs was recorded
        this._samplesPlayedAtLastFrame = 0;

        // Gate: main thread sends { type: 'start' } once pre-roll is satisfied
        this._started = false;

        this.port.onmessage = (e) => {
            const msg = e.data;
            if (msg.type === 'pcm') {
                // Transferred Float32Arrays — zero-copy from main thread
                this._pushChunk(msg.left, msg.right, msg.audioTimeMs);
            } else if (msg.type === 'start') {
                this._started = true;
                this._underrun = false;
                // Fade in from silence to avoid a click at the silence→audio boundary.
                this._fadeSamplesLeft = FADE_SAMPLES;
            } else if (msg.type === 'reset') {
                this._writeHead = 0;
                this._readHead = 0;
                this._started = false;
                this._underrun = false;
                this._fadeSamplesLeft = 0;
                this._samplesPlayed = 0;
                this._lastClockSamples = 0;
            }
        };
    }

    _pushChunk(left, right, audioTimeMs) {
        const n = left.length;
        const available = RING_FRAMES - ((this._writeHead - this._readHead + RING_FRAMES) % RING_FRAMES);
        if (n > available) {
            // Ring is full — this is a sign the consumer is paused or very slow.
            // Advance readHead to discard the oldest data so we don't block the writer.
            const discard = n - available + 256;
            this._readHead = (this._readHead + discard) % RING_FRAMES;
        }

        for (let i = 0; i < n; i++) {
            this._ringL[this._writeHead] = left[i];
            this._ringR[this._writeHead] = right[i];
            this._writeHead = (this._writeHead + 1) % RING_FRAMES;
        }

        // Record the clock anchor: the audioTimeMs of this frame and how many samples
        // have been played up to now — lets the main thread reconstruct the live clock.
        this._lastFrameAudioTimeMs = audioTimeMs;
        this._samplesPlayedAtLastFrame = this._samplesPlayed;
    }

    process(inputs, outputs) {
        const outL = outputs[0][0];
        const outR = outputs[0][1] || outputs[0][0]; // mono fallback
        const blockSize = outL.length; // always 128 on Web Audio

        if (!this._started) {
            // Pre-roll not satisfied — output silence, no clock messages
            outL.fill(0);
            outR.fill(0);
            return true;
        }

        const buffered = (this._writeHead - this._readHead + RING_FRAMES) % RING_FRAMES;

        if (buffered < blockSize) {
            // Underrun
            if (!this._underrun) {
                this._underrun = true;
                this._fadeSamplesLeft = FADE_SAMPLES;
                this.port.postMessage({ type: 'underrun', ringFill: buffered });
            }

            if (this._fadeSamplesLeft > 0) {
                // Drain whatever is left (may be < blockSize) with a linear fade-out
                const drain = Math.min(this._fadeSamplesLeft, buffered);
                for (let i = 0; i < drain; i++) {
                    const g = this._fadeSamplesLeft / FADE_SAMPLES;
                    outL[i] = this._ringL[this._readHead] * g;
                    outR[i] = this._ringR[this._readHead] * g;
                    this._readHead = (this._readHead + 1) % RING_FRAMES;
                    this._fadeSamplesLeft--;
                }
                // Fill remainder with silence
                for (let i = drain; i < blockSize; i++) {
                    outL[i] = 0;
                    outR[i] = 0;
                }
                this._samplesPlayed += blockSize;
            } else {
                outL.fill(0);
                outR.fill(0);
                this._samplesPlayed += blockSize;
            }

            return true;
        }

        // Normal playback — drain blockSize samples from ring
        if (this._underrun) {
            // Recovering from underrun — fade back in over FADE_SAMPLES
            this._underrun = false;
            this._fadeSamplesLeft = FADE_SAMPLES;
        }

        for (let i = 0; i < blockSize; i++) {
            let g = 1.0;
            if (this._fadeSamplesLeft > 0) {
                g = 1.0 - (this._fadeSamplesLeft / FADE_SAMPLES);
                this._fadeSamplesLeft--;
            }
            outL[i] = this._ringL[this._readHead] * g;
            outR[i] = this._ringR[this._readHead] * g;
            this._readHead = (this._readHead + 1) % RING_FRAMES;
        }

        this._samplesPlayed += blockSize;

        // Periodically post the clock so the main thread can steer video playbackRate
        if ((this._samplesPlayed - this._lastClockSamples) >= CLOCK_INTERVAL) {
            this._lastClockSamples = this._samplesPlayed;
            const fill = (this._writeHead - this._readHead + RING_FRAMES) % RING_FRAMES;
            this.port.postMessage({
                type: 'clock',
                audioTimeMs: this._lastFrameAudioTimeMs,
                samplesPlayedAtFrame: this._samplesPlayedAtLastFrame,
                samplesPlayed: this._samplesPlayed,
                ringFill: fill,
                ringCapacity: RING_FRAMES,
            });
        }

        return true;
    }
}

registerProcessor('pcm-sink', PcmSinkProcessor);
