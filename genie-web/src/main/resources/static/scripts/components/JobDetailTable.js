import React from 'react';
import { Link } from 'react-router'
import { render } from 'react-dom';
import $ from 'jquery';

export default class JobDetailTable extends React.Component {
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
    this.loadData(this.props.url);
  }

  componentWillReceiveProps(nextProps) {
    const { url } = nextProps;
    this.loadData(url);
  }

  loadData(url) {
    $.ajax({
      global: false,
      type: 'GET',
      headers: {
          'Accept': 'application/hal+json'
      },
      url: url
    }).done((job) => {
      this.setState({job});
    });
  }

  render() {
      return(
        <tr>
          <td colSpan="7">
            <button
              type="button"
              className="close pull-left"
              onClick={(e) => this.props.hideJobDetails()}
              aria-label="Close">
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
                </tbody>
              </table>
              </div>
          </td>
        </tr>
      );
  }
}
