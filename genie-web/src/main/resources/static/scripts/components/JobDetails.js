import React, { PropTypes as T } from 'react';

import { fetch } from '../utils';

export default class JobDetails extends React.Component {

  static propTypes = {
    row : T.object.isRequired,
  }

  constructor(props) {
    super(props);
    this.state = {
      killJobRequestSent: false,
      job: {
        id: '',
        _links: {
          output    : '',
          request   : '',
          execution : '',
          status    : '',
          self      : '',
        },
      },
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
    const url = row._links.self.href;

    fetch(url).done((job) => {
      this.setState({ job });
    });
  }

  killJob = (jobId) => {
    fetch(`/api/v3/jobs/${jobId}`, null, 'DELETE')
      .done(() => {
        this.setState({ killJobRequestSent: true });
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
                  <td className="col-xs-2">Job Id:</td>
                  <td>{this.state.job.id}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Version:</td>
                  <td>{this.state.job.version}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Description:</td>
                  <td>{this.state.job.description}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Tags:</td>
                  <td>{this.state.job.tags}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Status:</td>
                  <td>{this.state.job.status}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Status Msg:</td>
                  <td>{this.state.job.statusMsg}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Archive Location:</td>
                  <td>{this.state.job.archiveLocation}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Command Name:</td>
                  <td>{this.state.job.commandName}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Command Args:</td>
                  <td>{this.state.job.commandArgs}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Links:</td>
                  <td>
                    <ul>
                      <li><a target="_blank" href={this.state.job._links.self.href}>Json</a></li>
                      <li><a target="_blank" href={this.state.job._links.request.href}>Request</a></li>
                      <li><a target="_blank" href={this.state.job._links.execution.href}>Execution</a></li>
                      <li><a target="_blank" href={this.state.job._links.status.href}>Status </a></li>
                    </ul>
                  </td>
                </tr>
                {this.state.job.status === 'RUNNING' && !this.state.killJobRequestSent ?
                  <tr>
                    <td>
                      <button
                        type="button"
                        className="btn btn-danger"
                        onClick={() => this.killJob(this.state.job.id)}
                      >Send Kill Request</button>
                    </td>
                  </tr> : null
                }

              </tbody>
            </table>
            {this.state.killJobRequestSent ?
              <small>*Request accepted. Please refresh the page in a few seconds to see status change.</small> : null
            }
          </div>
        </td>
      </tr>
    );
  }
}
