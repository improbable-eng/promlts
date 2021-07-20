import React, { ChangeEvent, FC, useMemo, useState } from 'react';
import { RouteComponentProps } from '@reach/router';
import { UncontrolledAlert } from 'reactstrap';
import { useQueryParams, withDefault, NumberParam } from 'use-query-params';
import { withStatusIndicator } from '../../../components/withStatusIndicator';
import { useFetch } from '../../../hooks/useFetch';
import PathPrefixProps from '../../../types/PathPrefixProps';
import { Block } from './block';
import { SourceView } from './SourceView';
import { BlockDetails } from './BlockDetails';
import { BlockInput } from './BlockInput';
import { sortBlocks } from './helpers';
import styles from './blocks.module.css';
import TimeRange from './TimeRange';
export interface BlockListProps {
  blocks: Block[];
  err: string | null;
  label: string;
  refreshedAt: string;
}

export const BlocksContent: FC<{ data: BlockListProps }> = ({ data }) => {
  const [selectedBlock, selectBlock] = useState<Block>();
  const [searchState, setSearchState] = useState<string>('');
  const [blockSearch, setBlockSearch] = useState<string>('');

  const { blocks, label, err } = data;

  const blockPools = useMemo(() => sortBlocks(blocks, label), [blocks, label]);
  const [gridMinTime, gridMaxTime] = useMemo(() => {
    if (!err && blocks.length > 0) {
      let gridMinTime = blocks[0].minTime;
      let gridMaxTime = blocks[0].maxTime;
      blocks.forEach((block) => {
        if (block.minTime < gridMinTime) {
          gridMinTime = block.minTime;
        }
        if (block.maxTime > gridMaxTime) {
          gridMaxTime = block.maxTime;
        }
      });
      return [gridMinTime, gridMaxTime];
    }
    return [0, 0];
  }, [blocks, err]);

  const [{ 'min-time': viewMinTime, 'max-time': viewMaxTime }, setQuery] = useQueryParams({
    'min-time': withDefault(NumberParam, gridMinTime),
    'max-time': withDefault(NumberParam, gridMaxTime),
  });

  const setViewTime = (times: number[]): void => {
    setQuery({
      'min-time': times[0],
      'max-time': times[1],
    });
  };

  if (err) return <UncontrolledAlert color="danger">{err.toString()}</UncontrolledAlert>;

  return (
    <>
      <BlockInput
        onChange={({ target }: ChangeEvent<HTMLInputElement>): void => setSearchState(target.value)}
        onClick={() => setBlockSearch(searchState)}
      />
      {blocks.length > 0 ? (
        <div className={styles.container}>
          <div className={styles.grid}>
            <div className={styles.sources}>
              {Object.keys(blockPools).map((pk) => (
                <SourceView
                  key={pk}
                  data={blockPools[pk]}
                  title={pk}
                  selectBlock={selectBlock}
                  gridMinTime={viewMinTime}
                  gridMaxTime={viewMaxTime}
                  blockSearch={blockSearch}
                />
              ))}
            </div>
            <TimeRange
              gridMinTime={gridMinTime}
              gridMaxTime={gridMaxTime}
              viewMinTime={viewMinTime}
              viewMaxTime={viewMaxTime}
              onChange={setViewTime}
            />
          </div>
          <BlockDetails selectBlock={selectBlock} block={selectedBlock} />
        </div>
      ) : (
        <UncontrolledAlert color="warning">No blocks found.</UncontrolledAlert>
      )}
    </>
  );
};

const BlocksWithStatusIndicator = withStatusIndicator(BlocksContent);

interface BlocksProps {
  view?: string;
}

export const Blocks: FC<RouteComponentProps & PathPrefixProps & BlocksProps> = ({ pathPrefix = '', view = 'global' }) => {
  const { response, error, isLoading } = useFetch<BlockListProps>(
    `${pathPrefix}/api/v1/blocks${view ? '?view=' + view : ''}`
  );
  const { status: responseStatus } = response;
  const badResponse = responseStatus !== 'success' && responseStatus !== 'start fetching';

  return (
    <BlocksWithStatusIndicator
      data={response.data}
      error={badResponse ? new Error(responseStatus) : error}
      isLoading={isLoading}
    />
  );
};

export default Blocks;
