import React from 'react';
import { mount, ReactWrapper } from 'enzyme';
import { FetchMock } from 'jest-fetch-mock/types';
import { UncontrolledAlert } from 'reactstrap';
import Blocks from './Blocks';
import { SourceView } from './SourceView';
import { sampleAPIResponse } from './__testdata__/testdata';
import { act } from 'react-dom/test-utils';
import { PathPrefixContext } from '../../../contexts/PathPrefixContext';

describe('Blocks', () => {
  beforeEach(() => {
    fetchMock.resetMocks();
  });

  describe('when data is returned', () => {
    let blocks: ReactWrapper;
    let mock: FetchMock;

    beforeEach(() => {
      mock = fetchMock.mockResponse(JSON.stringify(sampleAPIResponse));
    });

    it('renders sources', async () => {
      await act(async () => {
        blocks = mount(
          <PathPrefixContext.Provider value="/path/prefix">
            <Blocks />
          </PathPrefixContext.Provider>
        );
      });
      blocks.update();
      expect(mock).toHaveBeenCalledWith('/path/prefix/../api/v1/blocks?view=global', {
        cache: 'no-store',
        credentials: 'same-origin',
      });

      const sourceViews = blocks.find(SourceView);
      expect(sourceViews).toHaveLength(8);
    });

    it('fetched data with different view', async () => {
      await act(async () => {
        blocks = mount(
          <PathPrefixContext.Provider value="/path/prefix">
            <Blocks view="loaded" />
          </PathPrefixContext.Provider>
        );
      });
      blocks.update();
      expect(mock).toHaveBeenCalledWith('/path/prefix/../api/v1/blocks?view=loaded', {
        cache: 'no-store',
        credentials: 'same-origin',
      });

      const sourceViews = blocks.find(SourceView);
      expect(sourceViews).toHaveLength(8);
    });
  });

  describe('when there are no blocks', () => {
    it('displays a warning alert', async () => {
      const mock = fetchMock.mockResponse(
        JSON.stringify({
          status: 'success',
          data: {
            blocks: [],
          },
        })
      );

      let blocks: any;
      await act(async () => {
        blocks = mount(
          <PathPrefixContext.Provider value="/path/prefix">
            <Blocks />
          </PathPrefixContext.Provider>
        );
      });
      blocks.update();

      expect(mock).toHaveBeenCalledWith('/path/prefix/../api/v1/blocks?view=global', {
        cache: 'no-store',
        credentials: 'same-origin',
      });

      const alert = blocks.find(UncontrolledAlert);
      expect(alert.prop('color')).toBe('warning');
      expect(alert.text()).toContain('No blocks found.');
    });
  });

  describe('when an error is returned', () => {
    it('displays an error alert', async () => {
      const mock = fetchMock.mockReject(new Error('Error fetching blocks'));

      let blocks: any;
      await act(async () => {
        blocks = mount(
          <PathPrefixContext.Provider value="/path/prefix">
            <Blocks />
          </PathPrefixContext.Provider>
        );
      });
      blocks.update();

      expect(mock).toHaveBeenCalledWith('/path/prefix/../api/v1/blocks?view=global', {
        cache: 'no-store',
        credentials: 'same-origin',
      });

      const alert = blocks.find(UncontrolledAlert);
      expect(alert.prop('color')).toBe('danger');
      expect(alert.text()).toContain('Error fetching blocks');
    });
  });
});
