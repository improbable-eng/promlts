import { LabelSet, Block, BlocksPool } from './block';

const stringify = (map: LabelSet): string => {
  let t = '';
  for (const [key, value] of Object.entries(map)) {
    t += `${key}: ${value} `;
  }
  return t;
};

export const isOverlapping = (a: Block, b: Block): boolean => {
  if (a?.minTime <= b?.minTime) return b?.minTime < a?.maxTime;
  else return a?.minTime < b?.maxTime;
};

const determineRow = (block: Block, rows: Block[][], startWithRow: number): number => {
  if (rows.length === 0) return 0;

  const len = rows[startWithRow]?.length || 0;
  if (len === 0) return startWithRow;

  if (isOverlapping(rows[startWithRow][len - 1], block)) {
    // Blocks are overlapping, try next row.
    return determineRow(block, rows, startWithRow + 1);
  }
  return startWithRow;
};

const splitOverlappingBlocks = (blocks: Block[]): Block[][] => {
  const rows: Block[][] = [[]];
  if (blocks.length === 0) return rows;

  blocks.forEach((b) => {
    const r = determineRow(b, rows, 0);
    if (!rows[r]) rows[r] = [];
    rows[r].push(b);
  });
  return rows;
};

const sortBlocksInRows = (blocks: Block[]): BlocksPool => {
  const poolWithOverlaps: { [key: string]: Block[] } = {};

  blocks
    .sort((a, b) => {
      if (a.compaction.level - b.compaction.level) {
        return a.compaction.level - b.compaction.level;
      }
      if (a.thanos.downsample.resolution - b.thanos.downsample.resolution) {
        return a.thanos.downsample.resolution - b.thanos.downsample.resolution;
      }
      return a.minTime - b.minTime;
    })
    .forEach((b) => {
      const key = `${b.compaction.level}-${b.thanos.downsample.resolution}`;
      if (!poolWithOverlaps[key]) poolWithOverlaps[key] = [];

      poolWithOverlaps[key].push(b);
    });

  const pool: BlocksPool = {};
  Object.entries(poolWithOverlaps).forEach(([key, blks]) => {
    pool[key] = splitOverlappingBlocks(blks);
  });

  return pool;
};

export const sortBlocks = (blocks: Block[], label: string): { [source: string]: BlocksPool } => {
  const titles: { [key: string]: string } = {};
  const pool: { [key: string]: Block[] } = {};

  blocks
    .sort((a, b) => a.compaction.level - b.compaction.level)
    .forEach((b) => {
      const title = (function (): string {
        const key = label !== '' && b.thanos.labels[label];

        if (key) {
          return key;
        } else {
          let t = titles[stringify(b.thanos.labels)];
          if (t === undefined) {
            t = String(Object.keys(titles).length + 1);
            titles[stringify(b.thanos.labels)] = t;
          }
          return t;
        }
      })();

      pool[title] = pool[title] ? pool[title].concat([b]) : [b];
    });

  const sortedPool: { [source: string]: BlocksPool } = {};
  Object.keys(pool).forEach((k) => {
    sortedPool[k] = sortBlocksInRows(pool[k]);
  });
  return sortedPool;
};

export const download = (blob: Block): string => {
  const url = window.URL.createObjectURL(new Blob([JSON.stringify(blob, null, 2)], { type: 'application/json' }));

  return url;
};

export const getOverlappingBlocks = (blockPools: { [source: string]: BlocksPool }): Set<string> => {
  let result: Set<string> = new Set();

  Object.keys(blockPools).forEach((pk) => {
    let blocks = Object.values(blockPools[pk]);
    for (let i = 0; i < blocks.length; i++) {
      if (blocks[i].length <= 1) {
        continue;
      }
      for (let j = 0; j < blocks[i].length - 1; j++) {
        for (let k = 0; k < blocks[i][j].length; k++) {
          for (let l = 0; l < blocks[i][j + 1].length; l++) {
            if (isOverlapping(blocks[i][j][k], blocks[i][j + 1][l])) {
              result.add(blocks[i][j][k].ulid);
              result.add(blocks[i][j + 1][l].ulid);
            }
          }
        }
      }
    }
  });

  return result;
};
