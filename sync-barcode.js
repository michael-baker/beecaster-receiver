/*
  sync-barcode.js — SINGLE SOURCE OF TRUTH for the A/V-sync VIDEO barcode.
  ============================================================================
  The on-screen barcode that carries the master audio-clock ms is rendered in TWO places that
  MUST stay byte-identical, or the phone decoder mis-reads and the self-test stops matching the
  real screen:
    * the on-TV CALIBRATION screen  (calibration.html)
    * the phone GENERATOR self-test (companion.html)

  PROTOCOL (the phone DECODER in companion.html — a separate responsibility — must AGREE with this):
    33 blocks = 8-bit sentinel 0xA5 + 24-bit ms (MSB-first) + 1 even-parity. white=1, black=0.

  QUANTIZATION: the ms is held to the nearest QUANT_MS so the fast low-order blocks stop churning
  every frame — a slow-GtG panel + long-exposure phone camera can then fully resolve them (big
  decode-rate win). The offset is constant and the companion medians over ~30 s, so the +/-QUANT_MS/2
  round-to-nearest dither averages out, only raising the spread floor to ~0.37*QUANT_MS ms. 64 keeps
  that under the 25 ms "high-confidence" bar while maximizing settled-frame share. If frames are still
  sparse, add LIGHT (shorter exposure) rather than raising QUANT_MS past ~67 (the 45 ms lock ceiling).
*/
(function (root) {
  'use strict';

  var SyncBarcode = {
    BLOCKS: 33,            // total blocks: 8 sentinel + 24 payload + 1 parity
    SENTINEL: 0xA5,        // 8-bit lead-in pattern
    PAYLOAD_BITS: 24,      // ms value, MSB-first (disambiguates any |offset| < 2^23 ms)
    MS_MODULO: 16777216,   // 2^24
    QUANT_MS: 64,          // temporal quantization (see header). Bisect 48 / 64 / 96.
    HEIGHT_FRACTION: 0.70, // share of the screen height the bar should fill (camera SNR + aim)

    // Round-to-nearest quantization -> zero-mean dither (no systematic offset bias).
    quantize: function (ms) {
      return Math.round(ms / SyncBarcode.QUANT_MS) * SyncBarcode.QUANT_MS;
    },

    // Render the barcode into a 2D context spanning W x H. white=1, black=0. The integer-block
    // render the calibration screen used is the SAME output as this when W is a multiple of BLOCKS
    // (which it arranges, cv.width = BLOCK_W * 33), so both screens stay pixel-identical.
    draw: function (g, W, H, msFull) {
      var ms = ((SyncBarcode.quantize(msFull) % SyncBarcode.MS_MODULO) + SyncBarcode.MS_MODULO) % SyncBarcode.MS_MODULO;
      var bw = W / SyncBarcode.BLOCKS, idx = 0, ones = 0, b, v;
      function blk(on) { g.fillStyle = on ? '#fff' : '#000'; g.fillRect(Math.floor(idx * bw), 0, Math.ceil(bw), H); idx++; }
      g.fillStyle = '#000'; g.fillRect(0, 0, W, H);
      for (b = 7; b >= 0; b--) blk((SyncBarcode.SENTINEL >> b) & 1);
      for (b = SyncBarcode.PAYLOAD_BITS - 1; b >= 0; b--) { v = (ms >> b) & 1; blk(v); ones += v; }
      blk(ones & 1);
    }
  };

  root.SyncBarcode = SyncBarcode;
})(typeof window !== 'undefined' ? window : this);
