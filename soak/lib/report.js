const fs = require('node:fs');

// The report as printed at the end of a run
function format(result) {
  const lines = [''];
  const headers = [
    'transport',
    'role',
    'messages',
    'MB',
    'msg/s',
    'MB/s',
    'CPU ms',
    'CPU µs/msg',
    'GC',
    'GC ms',
    'GC %',
    'peak RSS MB',
    'refused',
    'rechecked',
  ];
  const rows = result.rows.map((r) => {
    const seconds = r.elapsedMs / 1000;
    return [
      r.transport,
      r.role,
      String(r.messages),
      mb(r.bytes),
      (r.messages / seconds).toFixed(1),
      (r.bytes / 1048576 / seconds).toFixed(1),
      r.cpuMs.toFixed(0),
      r.messages ? ((r.cpuMs * 1000) / r.messages).toFixed(0) : '-',
      String(r.gcCount),
      r.gcMs.toFixed(0),
      r.cpuMs ? ((100 * r.gcMs) / r.cpuMs).toFixed(1) : '-',
      mb(r.peakRss),
      r.role === 'publisher' ? String(r.refused) : '-',
      r.role === 'consumer' ? String(r.reverified) : '-',
    ];
  });
  lines.push(...table([headers, ...rows]));

  if (result.failures.length > 0) {
    lines.push(
      '',
      `${result.failures.length} failure${result.failures.length === 1 ? '' : 's'}${result.failures.length >= 1000 ? ' (first 1000 kept)' : ''}:`,
    );
    const byKind = {};
    for (const f of result.failures) byKind[f.kind] = (byKind[f.kind] || 0) + 1;
    lines.push(
      `  by kind: ${Object.entries(byKind)
        .map(([k, n]) => `${k} ${n}`)
        .join(', ')}`,
    );
    for (const f of result.failures.slice(0, 20)) {
      lines.push(`  ${f.transport} ${f.queue} seq ${f.seq}: ${f.kind}: ${f.detail}`);
    }
    if (result.failures.length > 20) lines.push(`  ... and ${result.failures.length - 20} more`);
  }

  if (result.error) lines.push('', `error: ${result.error}`);

  lines.push('', `${result.verdict.toUpperCase()}  amqplib ${result.amqplib.version}, seed ${result.config.seed}`);
  return lines.join('\n');
}

function table(rows) {
  const widths = rows[0].map((_, col) => Math.max(...rows.map((r) => r[col].length)));
  return rows.map((r, i) => {
    const line = r.map((cell, col) => (col < 2 ? cell.padEnd(widths[col]) : cell.padStart(widths[col]))).join('  ');
    return i === 0 ? line : line;
  });
}

function mb(bytes) {
  return (bytes / 1048576).toFixed(1);
}

function writeJson(file, result) {
  fs.mkdirSync(require('node:path').dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(result, null, 2)}\n`);
}

module.exports = { format, writeJson };
