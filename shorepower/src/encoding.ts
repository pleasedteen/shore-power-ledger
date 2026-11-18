/**
 * 字符编码处理。
 *
 * 接电记录 CSV 为 GB18030 编码（中文表头/船名），优先使用 iconv-lite 解码；
 * 同时保留 Node 内置 TextDecoder（ICU 构建自带 gb18030）作为后备。
 */

import iconv from 'iconv-lite';

export function decodeGb18030(buf: Buffer): string {
  if (iconv.encodingExists('gb18030')) {
    return iconv.decode(buf, 'gb18030');
  }
  // 后备路径：Node 官方 ICU 构建通常内置 gb18030
  return new TextDecoder('gb18030').decode(buf);
}

export function encodeGb18030(text: string): Buffer {
  return iconv.encode(text, 'gb18030');
}

export function decodeUtf8(buf: Buffer): string {
  return new TextDecoder('utf-8').decode(buf);
}
