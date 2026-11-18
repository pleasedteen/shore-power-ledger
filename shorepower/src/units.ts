/**
 * 电量单位归一化与等值比较。
 *
 * 电表累计电量可能用 Wh 整数或小数 kWh 表示，统一换算为 Wh 后参与计算；
 * 不同单位表示的等值读数视为一致（在容差范围内）。
 */

export type EnergyUnit = 'Wh' | 'kWh';

/** 容差：1e-6 Wh，覆盖 kWh→Wh 的浮点尾差（0.001 kWh = 1 Wh） */
export const ENERGY_EPSILON_WH = 1e-6;

export function toWh(value: number, unit: EnergyUnit): number {
  return unit === 'kWh' ? value * 1000 : value;
}

/** 两个 Wh 值是否等值（不同单位换算后的尾差视为一致） */
export function energyEqual(aWh: number, bWh: number): boolean {
  return Math.abs(aWh - bWh) <= ENERGY_EPSILON_WH;
}

/** 用量展示：kWh = Wh/1000，保留 6 位小数 */
export function roundKwh(wh: number): number {
  return Math.round((wh / 1000) * 1e6) / 1e6;
}
