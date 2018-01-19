import T from "prop-types";
import React from "react";

import {
  momentFormat,
  fetch,
  activeClusterUrl,
  stripHateoasTemplateUrl
} from "../utils";

import $ from "jquery";

import InfoTable from "./InfoTable";

export default class CommandDetails extends React.Component {
  static propTypes = { row: T.object.isRequired };

  constructor(props) {
    super(props);
    this.state = {
      command: {
        configs: [],
        dependencies: [],
        _links: { self: "", clusters: "", applications: "" }
      },
      clusters: [],
      applications: []
    };
  }

  componentDidMount() {
    this.loadData(this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.loadData(nextProps);
  }

  loadData(props) {
    const { row } = props;
    const commandUrl = row._links.self.href;
    const clustersUrl = activeClusterUrl(row._links.clusters.href);
    const applicationsUrl = row._links.applications.href;
    const allClustersUrl = stripHateoasTemplateUrl(row._links.clusters.href);

    $.when(
      fetch(commandUrl),
      fetch(clustersUrl),
      fetch(applicationsUrl)
    ).done((command, clusters, applications) => {
      command[0]._links.clusters.href = allClustersUrl
      this.setState({
        command: command[0],
        clusters: clusters[0],
        applications: applications[0]
      });
    });
  }

  render() {
    return (
      <tr>
        <td colSpan="12">
          <i className="fa fa-sort-desc" aria-hidden="true" />
          <div className="commands-detail-row">
            <table className="table job-detail-table">
              <tbody>
                <tr>
                  <td className="col-xs-2 align-right">Description:</td>
                  <td>
                    {this.state.command.description}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Check Delay:</td>
                  <td>
                    {this.state.command.checkDelay}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Setup File:</td>
                  <td>
                    {this.state.command.setupFile}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Executable:</td>
                  <td>
                    {this.state.command.executable}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Config:</td>
                  <td>
                    <ul>
                      {this.state.command.configs.map((config, index) =>
                        <li key={index}>
                          {config}
                        </li>
                      )}
                    </ul>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Dependencies:</td>
                  <td>
                    <ul>
                      {this.state.command.dependencies.map(
                        (dependency, index) =>
                          <li key={index}>
                            {dependency}
                          </li>
                      )}
                    </ul>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Created:</td>
                  <td>
                    {momentFormat(this.state.command.created)}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Updated:</td>
                  <td>
                    {momentFormat(this.state.command.updated)}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Active Clusters:</td>
                  <td>
                    {this.state.clusters.length > 0
                      ? <InfoTable data={this.state.clusters} type="clusters" />
                      : <div />}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Applications:</td>
                  <td>
                    {this.state.applications.length > 0
                      ? <InfoTable
                          data={this.state.applications}
                          type="applications"
                        />
                      : <div />}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Links:</td>
                  <td>
                    <ul>
                      <li>
                        <a href={this.state.command._links.self.href}>Json</a>
                      </li>
                      <li>
                        <a href={this.state.command._links.applications.href}>
                          Applications
                        </a>
                      </li>
                      <li>
                        <a href={this.state.command._links.clusters.href}>
                          Clusters
                        </a>
                      </li>
                    </ul>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </td>
      </tr>
    );
  }
}
