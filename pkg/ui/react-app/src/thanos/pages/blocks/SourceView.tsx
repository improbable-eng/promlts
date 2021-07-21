import React, { FC } from 'react';
import { Block, BlocksPool } from './block';
import { BlockSpan } from './BlockSpan';
import styles from './blocks.module.css';

export const BlocksRow: FC<{
  blocks: Block[];
  gridMinTime: number;
  gridMaxTime: number;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
  overlappingBlocksId: Set<string>;
  findOverlapBlock: boolean;
}> = ({ blocks, gridMinTime, gridMaxTime, selectBlock, overlappingBlocksId, findOverlapBlock }) => {
  return (
    <div className={styles.row}>
      {blocks.map<JSX.Element | null>((b) => {
        if (overlappingBlocksId.has(b.ulid) || !findOverlapBlock) {
          return (
            <BlockSpan
              selectBlock={selectBlock}
              block={b}
              gridMaxTime={gridMaxTime}
              gridMinTime={gridMinTime}
              key={b.ulid}
            />
          );
        }
        return null;
      })}
    </div>
  );
};

export interface SourceViewProps {
  data: BlocksPool;
  title: string;
  gridMinTime: number;
  gridMaxTime: number;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
  findOverlapBlock: boolean;
  overlapBlocks: Set<string>;
}

export const SourceView: FC<SourceViewProps> = ({
  data,
  title,
  gridMaxTime,
  gridMinTime,
  selectBlock,
  findOverlapBlock,
  overlapBlocks,
}) => {
  return (
    <>
      <div className={styles.source}>
        <div className={styles.title} title={title}>
          <span>{title}</span>
        </div>
        <div className={styles.rowsContainer}>
          {Object.keys(data).map((k) => (
            <React.Fragment key={k}>
              {data[k].map((b, i) => (
                <BlocksRow
                  selectBlock={selectBlock}
                  blocks={b}
                  key={`${k}-${i}`}
                  gridMaxTime={gridMaxTime}
                  gridMinTime={gridMinTime}
                  findOverlapBlock={findOverlapBlock}
                  overlappingBlocksId={overlapBlocks}
                />
              ))}
            </React.Fragment>
          ))}
        </div>
      </div>
      <hr />
    </>
  );
};
