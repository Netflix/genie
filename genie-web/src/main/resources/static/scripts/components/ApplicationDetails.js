import React, { PropTypes } from 'react';

import { fetch } from '../utils';
import $ from 'jquery';

import InfoTable from './InfoTable';

export default class ApplicationDetails extends React.Component {

  static propTypes = {
    hideDetails : PropTypes.func.isRequired,
  }

  constructor(props) {
    super(props);
    this.state = {
      application: {
        configs: [],
        dependencies: [],
      },
      commands: [],
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
    const applicationUrl = row._links.self.href;
    const commandsUrl = row._links.commands.href;

    $.when(fetch(applicationUrl), fetch(commandsUrl)).done((application, commands) => {
      this.setState({
        application: application[0],
        commands: commands[0],
      });
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
                  <td className="col-xs-2">Type:</td>
                  <td>{this.state.application.type}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Description:</td>
                  <td>{this.state.application.description}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Setup File:</td>
                  <td>{this.state.application.setupFile}</td>
                </tr>
                <tr>
                  <td className="col-xs-2">Config:</td>
                  <td>
                    <ul>
                      {this.state.application.configs.map((config, index) =>
                        <li key={index}>{config}</li>
                      )}
                    </ul>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2">Dependencies:</td>
                  <td>
                    <ul>
                      {this.state.application.dependencies.map((data, index) =>
                        <li key={index}>{data}</li>
                      )}
                    </ul>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2">Commands:</td>
                  <td>
                    {this.state.commands.length > 0 ?
                      <InfoTable data={this.state.commands} type="commands" />
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

