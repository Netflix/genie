import React, { PropTypes as T } from 'react';

import { momentFormat, fetch } from '../utils';
import $ from 'jquery';

import InfoTable from './InfoTable';

export default class CommandDetails extends React.Component {

  static propTypes = {
    row : T.object.isRequired,
  }

  constructor(props) {
    super(props);
    this.state = {
      command: {
        configs: [],
      },
      clusters     : [],
      applications : [],
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
    const clustersUrl = `${row._links.clusters.href}?status=UP`;
    const applicationsUrl = row._links.applications.href;

    $.when(fetch(commandUrl), fetch(clustersUrl), fetch(applicationsUrl))
      .done((command, clusters, applications) => {
        this.setState({
          command      : command[0],
          clusters     : clusters[0],
          applications : applications[0],
        });
      });
  }

  render() {
    return (
      <tr>
        <td colSpan="12">
          <i className="fa fa-sort-desc" aria-hidden="true"></i>
          <div className="job-detail-row">
            <table className="table job-detail-table">
              <tbody>
                <tr>
                  <td className="col-xs-2">Description:</td>
                  <td>{this.state.command.description}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Check Delay:</td>
                  <td>{this.state.command.checkDelay}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Setup File:</td>
                  <td>{this.state.command.setupFile}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Executable:</td>
                  <td>{this.state.command.executable}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Config:</td>
                  <td>
                    <ul>
                      {this.state.command.configs.map((config, index) =>
                        <li key={index}>{config}</li>
                      )}
                    </ul>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2">Created:</td>
                  <td>{momentFormat(this.state.command.created)}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Updated:</td>
                  <td>{momentFormat(this.state.command.updated)}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Clusters:</td>
                  <td>
                    {this.state.clusters.length > 0 ?
                      <InfoTable data={this.state.clusters} type="clusters" />
                      : <div />
                    }
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2">Applications:</td>
                  <td>
                    {this.state.applications.length > 0 ?
                      <InfoTable data={this.state.applications} type="applications" />
                      : <div />
                    }
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
