import React from 'react';
import { mount, ReactWrapper } from 'enzyme';
import { FetchMock } from 'jest-fetch-mock/types';
import { UncontrolledAlert } from 'reactstrap';
import Stores from './Stores';
import StorePoolPanel from './StorePoolPanel';
import { sampleAPIResponse } from './__testdata__/testdata';
import { act } from 'react-dom/test-utils';
import { PathPrefixContext } from '../../../contexts/PathPrefixContext';

describe('Stores', () => {
  beforeEach(() => {
    fetchMock.resetMocks();
  });

  describe('when data is returned', () => {
    let stores: ReactWrapper;
    let mock: FetchMock;

    beforeEach(() => {
      mock = fetchMock.mockResponse(JSON.stringify(sampleAPIResponse));
    });

    it('renders tables', async () => {
      await act(async () => {
        stores = mount(
          <PathPrefixContext.Provider value="/path/prefix">
            <Stores />
          </PathPrefixContext.Provider>
        );
      });
      stores.update();
      expect(mock).toHaveBeenCalledWith('/path/prefix/../api/v1/stores', { cache: 'no-store', credentials: 'same-origin' });

      const panels = stores.find(StorePoolPanel);
      expect(panels).toHaveLength(2);

      const storePools = Object.keys(sampleAPIResponse.data);
      storePools.forEach(title => {
        const panel = stores.find(StorePoolPanel).filterWhere(panel => panel.prop('title') === title);
        expect(panel).toHaveLength(1);
      });
    });
  });

  describe('when there is no store registered', () => {
    it('displays a warning alert', async () => {
      const mock = fetchMock.mockResponse(
        JSON.stringify({
          status: 'success',
          data: [],
        })
      );

      let stores: any;
      await act(async () => {
        stores = mount(
          <PathPrefixContext.Provider value="/path/prefix">
            <Stores />
          </PathPrefixContext.Provider>
        );
      });
      stores.update();

      expect(mock).toHaveBeenCalledWith('/path/prefix/../api/v1/stores', { cache: 'no-store', credentials: 'same-origin' });

      const alert = stores.find(UncontrolledAlert);
      expect(alert.prop('color')).toBe('warning');
      expect(alert.text()).toContain('No stores registered');
    });
  });

  describe('when an error is returned', () => {
    it('displays an error alert', async () => {
      const mock = fetchMock.mockReject(new Error('Error fetching stores'));

      let stores: any;
      await act(async () => {
        stores = mount(
          <PathPrefixContext.Provider value="/path/prefix">
            <Stores />
          </PathPrefixContext.Provider>
        );
      });
      stores.update();

      expect(mock).toHaveBeenCalledWith('/path/prefix/../api/v1/stores', { cache: 'no-store', credentials: 'same-origin' });

      const alert = stores.find(UncontrolledAlert);
      expect(alert.prop('color')).toBe('danger');
      expect(alert.text()).toContain('Error fetching stores');
    });
  });
});
