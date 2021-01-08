import './globals';
import React from 'react';
import ReactDOM from 'react-dom';
import App from './App';
import 'bootstrap/dist/css/bootstrap.min.css';
import { isPresent } from './utils';

// Declared/defined in public/index.html, value replaced by Prometheus when serving bundle.
declare const THANOS_COMPONENT: string;

let thanosComponent = THANOS_COMPONENT;
if (THANOS_COMPONENT === '' || THANOS_COMPONENT === '{{ .Component }}') {
  thanosComponent = 'query';
}

ReactDOM.render(<App thanosComponent={thanosComponent} />, document.getElementById('root'));
