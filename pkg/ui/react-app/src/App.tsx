import React, { FC } from 'react';
import { Container } from 'reactstrap';
import { Router, Redirect, globalHistory } from '@reach/router';
import { QueryParamProvider } from 'use-query-params';

import { Alerts, Config, Flags, Rules, ServiceDiscovery, Status, Targets, TSDBStatus, PanelList, NotFound } from './pages';
import { PathPrefixContext } from './contexts/PathPrefixContext';
import ThanosComponentProps from './thanos/types/ThanosComponentProps';
import Navigation from './thanos/Navbar';
import { Stores, ErrorBoundary, Blocks } from './thanos/pages';

import './App.css';

const defaultRouteConfig: { [component: string]: string } = {
  query: '/graph',
  rule: '/alerts',
  bucket: '/blocks',
  compact: '/loaded',
  store: '/loaded',
};

const App: FC<ThanosComponentProps> = ({ thanosComponent }) => {
  // This dynamically/generically determines the pathPrefix by stripping the first known
  // endpoint suffix from the window location path. It works out of the box for both direct
  // hosting and reverse proxy deployments with no additional configurations required.
  let basePath = window.location.pathname;
  const paths = [
    '/graph',
    '/alerts',
    '/status',
    '/tsdb-status',
    '/flags',
    '/config',
    '/rules',
    '/targets',
    '/service-discovery',
    '/stores',
    '/blocks',
    '/loaded',
  ];
  if (basePath.endsWith('/')) {
    basePath = basePath.slice(0, -1);
  }
  if (basePath.length > 1) {
    for (let i = 0; i < paths.length; i++) {
      if (basePath.endsWith(paths[i])) {
        basePath = basePath.slice(0, basePath.length - paths[i].length);
        break;
      }
    }
  }

  return (
    <ErrorBoundary>
      <PathPrefixContext.Provider value={basePath}>
        <Navigation thanosComponent={thanosComponent} defaultRoute={defaultRouteConfig[thanosComponent]} />
        <Container fluid style={{ paddingTop: 70 }}>
          <QueryParamProvider reachHistory={globalHistory}>
            <Router basepath={`${basePath}`}>
              <Redirect from="/" to={`${basePath}${defaultRouteConfig[thanosComponent]}`} noThrow />

              <PanelList path="/graph" />
              <Alerts path="/alerts" />
              <Config path="/config" />
              <Flags path="/flags" />
              <Rules path="/rules" />
              <ServiceDiscovery path="/service-discovery" />
              <Status path="/status" />
              <TSDBStatus path="/tsdb-status" />
              <Targets path="/targets" />
              <Stores path="/stores" />
              <Blocks path="/blocks" />
              <Blocks path="/loaded" view="loaded" />
              <NotFound default defaultRoute={defaultRouteConfig[thanosComponent]} />
            </Router>
            .
          </QueryParamProvider>
        </Container>
      </PathPrefixContext.Provider>
    </ErrorBoundary>
  );
};

export default App;
