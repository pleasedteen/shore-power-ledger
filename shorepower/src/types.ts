/**
 * 全局类型定义。
 *
 * 数据管道的所有输入、结算结果、隔离结果与血缘都以显式类型描述，
 * 便于核对“日账/隔离/血缘”三者的一一对应关系。
 */

/** 一条原始记录的出处，用于血缘追溯 */
export interface SourceRef {
  /** 输入文件名（相对输入目录的路径） */
  file: string;
  /** 文件内容 SHA-256，识别重复/重送文件 */
  fileSha256: string;
  /** 记录在文件中的行号（从 1 开始；CSV 跳过表头，JSONL 按物理行） */
  line: number;
  /** 该来源记录的业务主键 */
  recordId: string;
  /** 该来源记录的修订版本号，越大越新 */
  revision: number;
  /** 该来源记录所属的可撤销来源批次（兼容既有非批次输入时缺省） */
  batchId?: string;
}

/** 可撤销来源批次的种类 */
export type SourceBatchKind = 'initial-import' | 'supplement' | 'correction' | 'legacy';

/** 批次状态：active 参与结算；revoked 已撤销，不再参与 */
export type SourceBatchStatus = 'active' | 'revoked';

/**
 * 可撤销的来源批次。
 *
 * 首次导入与补发各自形成一个可撤销批次：撤销某批次时，管道按“剩余有效批次”
 * 重新选出每次接电的最高修订，只重算真正受影响的接电。批次状态与发布结果一起
 * 落盘，因此撤销也是一次完整发布，中断后可重试。
 */
export interface SourceBatch {
  batchId: string;
  kind: SourceBatchKind;
  status: SourceBatchStatus;
  /** 关联的投递文件（相对输入目录）及其哈希；发布成功后投递文件被清走，台账仍保留 */
  files: InputFileInfo[];
  /** 首次登记时间 */
  createdAtMs: number;
  /** 最近一次状态变更时间 */
  updatedAtMs: number;
  /** 撤销时间（仅 status=revoked） */
  revokedAtMs?: number;
  /** 撤销操作备注（可选） */
  note?: string;
}

/** 撤销请求（由 CLI / API 传入） */
export interface RevokeRequest {
  /** 要撤销的批次；与 dryRun 组合可先预览影响范围 */
  batchId: string;
  /** 本次撤销的备注 */
  note?: string;
  /** 只预览、不发布 */
  dryRun?: boolean;
}

/** 批次台账：随发布目录持久化，保证撤销可重入、可追溯 */
export interface BatchLedger {
  batches: SourceBatch[];
}

/**
 * 批次里的一条原始记录（接电或读数）。
 *
 * 投递文件在发布成功后会被清走，撤销批次时不能再依赖输入目录；
 * 因此记录内容随批次一起落盘，撤销只读这里涉及的主键版本与相关接电的结算输入。
 */
export interface BatchRecord {
  batchId: string;
  kind: 'connection' | 'reading';
  record: ConnectionRecord | MeterReading;
}

/** 撤销/重试计划的执行结果 */
export interface RevokeOutcome {
  batchId: string;
  dryRun: boolean;
  /** 撤销后不再参与结算的接电（因该批次记录被移除而修订回退或消失） */
  affectedConnectionIds: string[];
  /** 其中实际重算并替换日账/隔离/血缘的接电 */
  recomputedConnectionIds: string[];
  /** 撤销后若无有效批次来源而被保留为历史的接电（无损可查） */
  withdrawnDocIds: string[];
  /** 尚未发布（dryRun）时为 null */
  publicationId: string | null;
  skipped: boolean;
  warnings: string[];
}

/** 输入文件扫描信息，写入发布清单 */
export interface InputFileInfo {
  file: string;
  encoding: 'GB18030' | 'UTF-8';
  sha256: string;
  sizeBytes: number;
  /** 内容完全相同的重复文件（同哈希），扫描时只处理第一个 */
  duplicateOf?: string;
  /** 该文件所属的可撤销来源批次（首次导入 / 补发 / 更正批次） */
  batchId?: string;
}

/** 接电记录（来自 GB18030 CSV，时间为北京时间 UTC+8） */
export interface ConnectionRecord {
  /** 接电记录主键，同一 connectionId 以最高 revision 为准 */
  connectionId: string;
  revision: number;
  vessel: string;
  meterId: string;
  /** 接电起始时刻，北京时间（UTC+8）对应的绝对时间 ms */
  startBeijingMs: number;
  /** 接电结束时刻，北京时间（UTC+8）对应的绝对时间 ms */
  endBeijingMs: number;
  sources: SourceRef[];
}

/** 电表读数（来自 UTF-8 JSONL，采样时间为 UTC） */
export interface MeterReading {
  /** 读数记录主键（同一 readingId 以最高 revision 为准）；补发数据可为新 id */
  readingId: string;
  revision: number;
  meterId: string;
  /** 采样时刻（UTC）对应的绝对时间 ms */
  sampledAtMs: number;
  /** 累计电量，统一归一化为 Wh */
  cumulativeWh: number;
  /** 原始单位，仅用于血缘与展示 */
  originalUnit: 'Wh' | 'kWh';
  /** 原始数值（字符串保留原始写法） */
  originalValue: string;
  sources: SourceRef[];
}

/** 隔离原因码 */
export type QuarantineReason =
  | 'INVALID_CONNECTION_WINDOW'
  | 'MISSING_START_READING'
  | 'MISSING_END_READING'
  | 'MISSING_MIDNIGHT_READING'
  | 'CONFLICTING_READING'
  | 'CUMULATIVE_REGRESSION'
  | 'SOURCE_REVISION_CONFLICT';

/** 一次结算所依据的边界读数 */
export interface BoundaryPoint {
  /** 边界语义 */
  kind: 'CONNECTION_START' | 'BEIJING_MIDNIGHT' | 'CONNECTION_END';
  /** 边界对应的北京时间自然日（仅午夜点使用） */
  day?: string;
  sampledAtMs: number;
  cumulativeWh: number;
  source: SourceRef;
}

/** 日账文档：一次接电 × 一个北京时间自然日 */
export interface DailyAccount {
  docType: 'daily-account';
  docId: string;
  publicationId: string;
  connectionId: string;
  revision: number;
  vessel: string;
  meterId: string;
  /** 北京时间自然日，YYYY-MM-DD */
  beijingDay: string;
  /** 当日在接电区间内的起止（绝对时间 ms） */
  windowStartMs: number;
  windowEndMs: number;
  /** 当日用量 Wh（始终给出） */
  usageWh: number;
  /** 当日用量 kWh（派生值，保留 6 位小数） */
  usageKwh: number;
  startBoundary: BoundaryPoint;
  endBoundary: BoundaryPoint;
  /** 该日窗口内参与计算的全部读数（含两端），按采样时间升序 */
  readingsUsed: Array<{
    sampledAtMs: number;
    cumulativeWh: number;
    source: SourceRef;
  }>;
}

/** 隔离文档：整次接电进入隔离区 */
export interface QuarantineEntry {
  docType: 'quarantine';
  docId: string;
  publicationId: string;
  connectionId: string;
  revision: number;
  vessel: string;
  meterId: string;
  startBeijingMs: number;
  endBeijingMs: number;
  reasons: QuarantineReason[];
  /** 人类可读的中文说明 */
  messages: string[];
  /** 触发问题时涉及的读数出处 */
  evidence: Array<{
    sampledAtMs?: number;
    cumulativeWh?: number;
    note: string;
    source?: SourceRef;
  }>;
}

/** 血缘文档：每个日账或隔离结果对应一条 */
export interface LineageRecord {
  docType: 'lineage';
  docId: string;
  publicationId: string;
  /** 被追溯的结果文档 id */
  subjectDocId: string;
  subjectKind: 'daily-account' | 'quarantine';
  connectionId: string;
  /** 依据的接电记录（最高修订）出处 */
  connectionSources: SourceRef[];
  /** 依据的全部读数记录出处（去重后） */
  readingSources: SourceRef[];
  /** 本次发布扫描到的输入文件清单 */
  inputFiles: InputFileInfo[];
}

/** 发布清单：一次完整发布的元数据 */
export interface PublicationManifest {
  docType: 'manifest';
  publicationId: string;
  createdAtMs: number;
  /**
   * full：全量发布；rerun：指定日期重跑；supplement：补发批次；
   * correction：更正批次；revoke：撤销来源批次后按剩余有效批次重算
   */
  mode: 'full' | 'rerun' | 'supplement' | 'correction' | 'revoke';
  /** rerun 时运营人员指定的北京时间日期 */
  rerunDay?: string;
  inputFiles: InputFileInfo[];
  accountCount: number;
  quarantineCount: number;
  /** 本次纳入结算的有效（未撤销）来源批次 */
  activeBatchIds: string[];
  /** 本次撤销的批次（含此前已撤销的累积集合） */
  revokedBatchIds: string[];
  /** 本次发布涉及（重新计算）的接电 id */
  recomputedConnectionIds: string[];
  /** 沿用上一版结果的接电 id */
  carriedConnectionIds: string[];
  /** 本次被撤下的旧结果文档 id（旧版接电时段不再被纳入） */
  withdrawnDocIds: string[];
  /** 采集阶段的告警（如同主键同修订冲突） */
  warnings: string[];
  /** 发布业务内容指纹，用于相同输入重跑时跳过重复发布 */
  fingerprint: string;
}

/** 单次接电的结算产物（发布前形态，publicationId 由管线填充） */
export interface SettlementResult {
  connection: ConnectionRecord;
  account?: Omit<DailyAccount, 'publicationId'>;
  quarantine?: Omit<QuarantineEntry, 'publicationId'>;
  /** 血缘中引用的全部读数出处 */
  readingSources: SourceRef[];
}
