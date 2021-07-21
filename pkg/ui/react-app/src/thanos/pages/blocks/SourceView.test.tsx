import React from 'react';
import { mount } from 'enzyme';
import { SourceView, SourceViewProps, BlocksRow } from './SourceView';
import { sampleAPIResponse } from './__testdata__/testdata';
import { sortBlocks } from './helpers';

const sorted = sortBlocks(sampleAPIResponse.data.blocks, sampleAPIResponse.data.label);
const source = 'prometheus_one';

describe('Blocks SourceView', () => {
  const defaultProps: SourceViewProps = {
    title: source,
    data: sorted[source],
    selectBlock: (): void => {
      // do nothing
    },
    gridMinTime: 1596096000000,
    gridMaxTime: 1595108031471,
    findOverlapBlock: false,
    overlapBlocks: new Set(),
  };

  const sourceView = mount(<SourceView {...defaultProps} />);

  it('renders a paragraph with title', () => {
    const title = sourceView.find('div > span');
    expect(title).toHaveLength(1);
    expect(title.text()).toEqual(source);
  });

  it('renders a row for each unique resolution and compaction level pair', () => {
    const rows = sourceView.find(BlocksRow);
    expect(rows).toHaveLength(Object.keys(sorted[source]).length);
  });
});
