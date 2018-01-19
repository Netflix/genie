import T from "prop-types";
import React from "react";

import {
  momentFormat,
  fetch,
  activeCommandUrl,
  activeClusterUrl,
  stripHateoasTemplateUrl
} from "../utils";

import $ from "jquery";

import InfoTable from "./InfoTable";

export default class ClusterDetails extends React.Component {
  static propTypes = { row: T.object.isRequired };

  constructor(props) {
    super(props);
    this.state = {
      cluster: {
        id: "",
        configs: [],
        dependencies: [],
        _links: { self: "", commands: "" }
      },
      commands: []
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
    const clusterUrl = activeClusterUrl(row._links.self.href);
    const commandsUrl = activeCommandUrl(row._links.commands.href);
    const allCommandsUrl = stripHateoasTemplateUrl(row._links.commands.href)

    $.when(fetch(clusterUrl), fetch(commandsUrl)).done((cluster, commands) => {
      cluster[0]._links.commands.href = allCommandsUrl;
      this.setState({ cluster: cluster[0], commands: commands[0] });
    });
  }

  render() {
    return (
      <tr>
        <td colSpan="12">
          <i className="fa fa-sort-desc" aria-hidden="true" />
          <div className="job-detail-row">
            <table className="table job-detail-table">
              <tbody>
                <tr>
                  <td className="col-xs-2 align-right">Description:</td>
                  <td>
                    {this.state.cluster.description}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Setup File:</td>
                  <td>
                    {this.state.cluster.setupFile}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Config:</td>
                  <td>
                    <ul>
                      {this.state.cluster.configs.map((config, index) =>
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
                      {this.state.cluster.dependencies.map((data, index) =>
                        <li key={index}>
                          {data}
                        </li>
                      )}
                    </ul>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Created:</td>
                  <td>
                    {momentFormat(this.state.cluster.created)}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Updated:</td>
                  <td>
                    {momentFormat(this.state.cluster.updated)}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Active Commands:</td>
                  <td>
                    {this.state.commands.length > 0
                      ? <InfoTable data={this.state.commands} type="commands" />
                      : <div />}
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Links:</td>
                  <td>
                    <ul>
                      <li>
                        <a href={this.state.cluster._links.self.href}>Json</a>
                      </li>
                      <li>
                        <a href={this.state.cluster._links.commands.href}>
                          Commands
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
