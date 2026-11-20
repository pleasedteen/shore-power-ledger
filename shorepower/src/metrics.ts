/**
 * 明细读写计量器。
 *
 * “明细”特指发布目录中的日账 / 隔离 / 血缘文档（不含 manifest、index 等元数据）。
 * 计量器的目的：证明按天查询与指定日重跑只触碰少量明细，而不是整版。
 *
 * 统计口径：
 * - detailDocsRead：从发布分片中解析出的明细文档条数；
 * - detailDocsWritten：本次发布真正序列化写出的明细文档条数（复用的硬链接不计）；
 * - shardsLinked：沿用上一版、以硬链接复用的分片文件数。
 */

export interface DetailIoSnapshot {
  /** 读到的明细文档条数 */
  detailDocsRead: number;
  /** 写出的明细文档条数 */
  detailDocsWritten: number;
  /** 明细分片读取字节数 */
  shardBytesRead: number;
  /** 明细分片写入字节数 */
  shardBytesWritten: number;
  /** 读取的明细分片文件数 */
  shardsRead: number;
  /** 写入的明细分片文件数 */
  shardsWritten: number;
  /** 以硬链接复用的分片文件数（不产生明细写入） */
  shardsLinked: number;
}

function emptySnapshot(): DetailIoSnapshot {
  return {
    detailDocsRead: 0,
    detailDocsWritten: 0,
    shardBytesRead: 0,
    shardBytesWritten: 0,
    shardsRead: 0,
    shardsWritten: 0,
    shardsLinked: 0,
  };
}

export class DetailIoCounter {
  private counts = emptySnapshot();

  addShardRead(docs: number, bytes: number): void {
    this.counts.detailDocsRead += docs;
    this.counts.shardBytesRead += bytes;
    this.counts.shardsRead += 1;
  }

  addShardWrite(docs: number, bytes: number): void {
    this.counts.detailDocsWritten += docs;
    this.counts.shardBytesWritten += bytes;
    this.counts.shardsWritten += 1;
  }

  addLinkedShard(): void {
    this.counts.shardsLinked += 1;
  }

  snapshot(): DetailIoSnapshot {
    return { ...this.counts };
  }
}

/** 合并多个计量快照（例如一次请求内多个分片读取） */
export function mergeDetailIo(...parts: DetailIoSnapshot[]): DetailIoSnapshot {
  const out = emptySnapshot();
  for (const p of parts) {
    out.detailDocsRead += p.detailDocsRead;
    out.detailDocsWritten += p.detailDocsWritten;
    out.shardBytesRead += p.shardBytesRead;
    out.shardBytesWritten += p.shardBytesWritten;
    out.shardsRead += p.shardsRead;
    out.shardsWritten += p.shardsWritten;
    out.shardsLinked += p.shardsLinked;
  }
  return out;
}
