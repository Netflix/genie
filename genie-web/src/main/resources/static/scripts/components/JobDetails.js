import React, { PropTypes } from 'react';

import { fetch } from '../utils';

export default class JobDetails extends React.Component {

  static propTypes = {
    jobUrl      : PropTypes.string.isRequired,
    killJob     : PropTypes.func.isRequired,
    hideDetails : PropTypes.func.isRequired,
  }

  constructor(props) {
    super(props);
    this.state = {
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
    this.loadData(this.props.jobUrl);
  }

  componentWillReceiveProps(nextProps) {
    const { jobUrl } = nextProps;
    this.loadData(jobUrl);
  }

  loadData(url) {
    fetch(url).done((job) => {
      this.setState({ job });
    });
  }

  render() {
    return (
      <tr>
        <td colSpan="12">
          <button
            type="button"
            className="close pull-left"
            onClick={() => this.props.hideDetails()}
            aria-label="Close"
          >
            <i className="fa fa-times" aria-hidden="true"></i>
          </button>
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
                {this.state.job.status === 'RUNNING' ?
                  <tr>
                    <td>
                      <button
                        type="button"
                        className="btn btn-danger"
                        onClick={() => this.props.killJob(this.state.job.id)}
                      >Kill</button>
                    </td>
                  </tr> : null
                }
              </tbody>
            </table>
          </div>
        </td>
      </tr>
    );
  }
}
