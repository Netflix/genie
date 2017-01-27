import React from "react";
import { render } from "react-dom";
import { Router, Route, browserHistory, IndexRedirect } from "react-router";

import App from "./scripts/Main";
import Job from "./scripts/Job";
import Cluster from "./scripts/Cluster";
import Command from "./scripts/Command";
import Application from "./scripts/Application";
import OutputDirectory from "./scripts/OutputDirectory";

import "./styles/font-awesome.min.css";
import "./styles/bootstrap.min.css";
import "./styles/genie.css";

render(
  <Router history={browserHistory}>
    <Route path="/" component={App}>
      <IndexRedirect to="/jobs" />
      <Route path="/jobs" component={Job} />
      <Route path="/clusters" component={Cluster} />
      <Route path="/commands" component={Command} />
      <Route path="/applications" component={Application} />
    </Route>
    <Route path="/output/*" component={OutputDirectory} />
  </Router>,
  document.getElementById("root")
);
