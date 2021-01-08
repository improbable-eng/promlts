import React, { FC, useState, useEffect } from 'react';
import { RouteComponentProps } from '@reach/router';
import { UncontrolledAlert, Button } from 'reactstrap';

import Panel, { PanelOptions, PanelDefaultOptions } from './Panel';
import Checkbox from '../../components/Checkbox';
import { StoreListProps } from '../../thanos/pages/stores/Stores';
import { Store } from '../../thanos/pages/stores/store';
import { generateID, decodePanelOptionsFromQueryString, encodePanelOptionsToQueryString, callAll } from '../../utils';
import { useFetch } from '../../hooks/useFetch';
import { useLocalStorage } from '../../hooks/useLocalStorage';
import { withStatusIndicator } from '../../components/withStatusIndicator';
import { usePathPrefix } from '../../contexts/PathPrefixContext';
import { API_PATH } from '../../constants/constants';

export type PanelMeta = { key: string; options: PanelOptions; id: string };

export const updateURL = (nextPanels: PanelMeta[]) => {
  const query = encodePanelOptionsToQueryString(nextPanels);
  window.history.pushState({}, '', query);
};

interface PanelListProps extends RouteComponentProps {
  panels: PanelMeta[];
  metrics: string[];
  useLocalTime: boolean;
  queryHistoryEnabled: boolean;
  stores: StoreListProps;
  enableAutocomplete: boolean;
}

export const PanelListContent: FC<PanelListProps> = ({
  metrics = [],
  useLocalTime,
  queryHistoryEnabled,
  stores = {},
  enableAutocomplete,
  ...rest
}) => {
  const [panels, setPanels] = useState(rest.panels);
  const [historyItems, setLocalStorageHistoryItems] = useLocalStorage<string[]>('history', []);
  const [storeData, setStoreData] = useState([] as Store[]);

  useEffect(() => {
    // Convert stores data to a unified stores array.
    const storeList: Store[] = [];
    for (const type in stores) {
      storeList.push(...stores[type]);
    }
    setStoreData(storeList);
    // Clear selected stores for each panel.
    panels.forEach((panel: PanelMeta) => (panel.options.storeMatches = []));
    !panels.length && addPanel();
    window.onpopstate = () => {
      const panels = decodePanelOptionsFromQueryString(window.location.search);
      if (panels.length > 0) {
        setPanels(panels);
      }
    };
    // We want useEffect to act only as componentDidMount, but react still complains about the empty dependencies list.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stores]);

  const handleExecuteQuery = (query: string) => {
    const isSimpleMetric = metrics.indexOf(query) !== -1;
    if (isSimpleMetric || !query.length) {
      return;
    }
    const extendedItems = historyItems.reduce(
      (acc, metric) => {
        return metric === query ? acc : [...acc, metric]; // Prevent adding query twice.
      },
      [query]
    );
    setLocalStorageHistoryItems(extendedItems.slice(0, 50));
  };

  const addPanel = () => {
    callAll(
      setPanels,
      updateURL
    )([
      ...panels,
      {
        id: generateID(),
        key: `${panels.length}`,
        options: PanelDefaultOptions,
      },
    ]);
  };

  const pathPrefix = usePathPrefix();

  return (
    <>
      {panels.map(({ id, options }) => (
        <Panel
          pathPrefix={pathPrefix}
          onExecuteQuery={handleExecuteQuery}
          key={id}
          options={options}
          id={id}
          onOptionsChanged={opts =>
            callAll(setPanels, updateURL)(panels.map(p => (id === p.id ? { ...p, options: opts } : p)))
          }
          removePanel={() =>
            callAll(
              setPanels,
              updateURL
            )(
              panels.reduce<PanelMeta[]>(
                (acc, panel) => (panel.id !== id ? [...acc, { ...panel, key: `${acc.length}` }] : acc),
                []
              )
            )
          }
          useLocalTime={useLocalTime}
          metricNames={metrics}
          pastQueries={queryHistoryEnabled ? historyItems : []}
          stores={storeData}
          enableAutocomplete={enableAutocomplete}
        />
      ))}
      <Button className="mb-3" color="primary" onClick={addPanel}>
        Add Panel
      </Button>
    </>
  );
};

const PanelListContentWithIndicator = withStatusIndicator(PanelListContent);

const PanelList: FC<RouteComponentProps> = () => {
  const [delta, setDelta] = useState(0);
  const [useLocalTime, setUseLocalTime] = useLocalStorage('use-local-time', false);
  const [enableQueryHistory, setEnableQueryHistory] = useLocalStorage('enable-query-history', false);
  const [debugMode, setDebugMode] = useState(false);
  const [enableAutocomplete, setEnableAutocomplete] = useLocalStorage('enable-autocomplete', true);

  const pathPrefix = usePathPrefix();
  const { response: metricsRes, error: metricsErr } = useFetch<string[]>(`${pathPrefix}/${API_PATH}/label/__name__/values`);
  const { response: storesRes, error: storesErr, isLoading: storesLoading } = useFetch<StoreListProps>(
    `${pathPrefix}/${API_PATH}/stores`
  );

  const browserTime = new Date().getTime() / 1000;
  const { response: timeRes, error: timeErr } = useFetch<{ result: number[] }>(
    `${pathPrefix}/${API_PATH}/query?query=time()`
  );

  useEffect(() => {
    if (timeRes.data) {
      const serverTime = timeRes.data.result[0];
      setDelta(Math.abs(browserTime - serverTime));
    }
    /**
     * React wants to include browserTime to useEffect dependencies list which will cause a delta change on every re-render
     * Basically it's not recommended to disable this rule, but this is the only way to take control over the useEffect
     * dependencies and to not include the browserTime variable.
     **/
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [timeRes.data]);

  return (
    <>
      <Checkbox
        wrapperStyles={{ marginLeft: 3, display: 'inline-block' }}
        id="query-history-checkbox"
        onChange={({ target }) => setEnableQueryHistory(target.checked)}
        defaultChecked={enableQueryHistory}
      >
        Enable query history
      </Checkbox>
      <Checkbox
        wrapperStyles={{ marginLeft: 20, display: 'inline-block' }}
        id="use-local-time-checkbox"
        onChange={({ target }) => setUseLocalTime(target.checked)}
        defaultChecked={useLocalTime}
      >
        Use local time
      </Checkbox>
      <Checkbox
        wrapperStyles={{ marginLeft: 20, display: 'inline-block' }}
        id="debug-mode-checkbox"
        defaultChecked={debugMode}
        onChange={({ target }) => setDebugMode(target.checked)}
      >
        Enable Store Filtering
      </Checkbox>
      <Checkbox
        wrapperStyles={{ marginLeft: 20, display: 'inline-block' }}
        id="autocomplete"
        defaultChecked={enableAutocomplete}
        onChange={({ target }) => setEnableAutocomplete(target.checked)}
      >
        Enable autocomplete
      </Checkbox>
      {(delta > 30 || timeErr) && (
        <UncontrolledAlert color="danger">
          <strong>Warning: </strong>
          {timeErr && `Unexpected response status when fetching server time: ${timeErr.message}`}
          {delta >= 30 &&
            `Error fetching server time: Detected ${delta} seconds time difference between your browser and the server. Thanos relies on accurate time and time drift might cause unexpected query results.`}
        </UncontrolledAlert>
      )}
      {metricsErr && (
        <UncontrolledAlert color="danger">
          <strong>Warning: </strong>
          Error fetching metrics list: Unexpected response status when fetching metric names: {metricsErr.message}
        </UncontrolledAlert>
      )}
      {storesErr && (
        <UncontrolledAlert color="danger">
          <strong>Warning: </strong>
          Error fetching stores list: Unexpected response status when fetching stores: {storesErr.message}
        </UncontrolledAlert>
      )}
      <PanelListContentWithIndicator
        panels={decodePanelOptionsFromQueryString(window.location.search)}
        useLocalTime={useLocalTime}
        metrics={metricsRes.data}
        stores={debugMode ? storesRes.data : {}}
        enableAutocomplete={enableAutocomplete}
        queryHistoryEnabled={enableQueryHistory}
        isLoading={storesLoading}
      />
    </>
  );
};

export default PanelList;
