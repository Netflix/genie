import React from 'react';
import {render} from 'react-dom';
import { Router, Route, browserHistory, Redirect } from 'react-router';

import App from './scripts/App';
import Job from './scripts/Job';
import Cluster from './scripts/Cluster';
import Command from './scripts/Command';
import Application from './scripts/Application';
import GenieJobDirectory from './scripts/GenieJobDirectory';

import './styles/font-awesome.min.css';
import './styles/bootstrap.min.css';
import './styles/genie.css';

render((
  <Router history={browserHistory}>
    <Route path="/" component={App}>
    	<Route path="/jobs" component={Job}></Route>
    	<Route path="/clusters" component={Cluster}></Route>
    	<Route path="/commands" component={Command}></Route>
    	<Route path="/applications" component={Application}></Route>
    </Route>
    <Route path="/genie-jobs/*" component={GenieJobDirectory}></Route>
  </Router>
), document.getElementById('root'));
