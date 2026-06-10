/*
  A/V-sync video barcode carrying the master audio-clock ms.

  Protocol: 33 blocks = 8-bit sentinel 0xA5 + 24-bit ms (MSB-first) + 1 even-parity. white=1, black=0.

  ms is quantized to the nearest QUANT_MS so the low-order blocks stop churning every frame,
  letting a slow panel and long-exposure camera resolve them.
*/
(function (root) {
  'use strict';

  var SyncBarcode = {
    BLOCKS: 33,            // 8 sentinel + 24 payload + 1 parity
    SENTINEL: 0xA5,        // 8-bit lead-in pattern
    PAYLOAD_BITS: 24,      // ms value, MSB-first
    MS_MODULO: 16777216,   // 2^24
    QUANT_MS: 64,          // temporal quantization, see header
    HEIGHT_FRACTION: 0.70, // share of screen height the bar fills

    quantize: function (ms) {
      return Math.round(ms / SyncBarcode.QUANT_MS) * SyncBarcode.QUANT_MS;
    },

    // Render into a 2D context spanning W x H. white=1, black=0.
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
