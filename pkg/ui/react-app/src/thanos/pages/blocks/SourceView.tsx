import React, { FC, useRef, useState } from 'react';
import { Block, BlocksPool } from './block';
import { BlockSpan } from './BlockSpan';
import styles from './blocks.module.css';
import { Tooltip } from 'reactstrap';

export const BlocksRow: FC<{
  blocks: Block[];
  gridMinTime: number;
  gridMaxTime: number;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
}> = ({ blocks, gridMinTime, gridMaxTime, selectBlock }) => {
  return (
    <div className={styles.row}>
      {blocks.map<JSX.Element>((b) => (
        <BlockSpan selectBlock={selectBlock} block={b} gridMaxTime={gridMaxTime} gridMinTime={gridMinTime} key={b.ulid} />
      ))}
    </div>
  );
};

export interface SourceViewProps {
  data: BlocksPool;
  title: string;
  gridMinTime: number;
  gridMaxTime: number;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
  blockCount: { [key: string]: number };
}

export const SourceView: FC<SourceViewProps> = ({ data, title, gridMaxTime, gridMinTime, selectBlock, blockCount }) => {
  const linkRef = useRef<HTMLSpanElement>();
  const [open, setOpen] = useState(false);
  const toggle = () => setOpen(!open);

  return (
    <>
      <div className={styles.source}>
        <div className={styles.title} title={title}>
          <span ref={linkRef}>{title}</span>
          {linkRef.current && (
            <Tooltip placement="right" isOpen={open} target={linkRef.current} toggle={toggle} autohide={false}>
              Blocks Count: {blockCount[title]}
            </Tooltip>
          )}
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
