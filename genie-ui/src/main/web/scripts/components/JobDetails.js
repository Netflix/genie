import T from "prop-types";
import React from "react";
import { Link } from "react-router";

import { momentFormat, fetch } from "../utils";
import Modal from "react-modal";
import Error from "./Error";

import $ from "jquery";

export default class JobDetails extends React.Component {
  static propTypes = { row: T.object.isRequired };

  constructor(props) {
    super(props);
    this.state = {
      killJobRequestSent: false,
      modalIsOpen: false,
      killRequestError: null,
      command: { id: "", version: "", name: "" },
      cluster: { id: "", version: "", name: "" },
      applications: [],
      job: {
        id: "",
        tags: [],
        _links: {
          output: "",
          request: "",
          execution: "",
          status: "",
          self: "",
          command: "",
          cluster: "",
          applications: ""
        }
      }
    };
    window.myState = this.state;
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
    fetch(url).done(job => {
      this.setState({ job });

      $.when(
        fetch(job._links.cluster.href),
        fetch(job._links.command.href),
        fetch(job._links.applications.href)
      )
        .done((clusters, commands, applications) => {
          console.log(applications);
          this.setState({
            cluster: clusters[0],
            command: commands[0],
            applications: applications[0]
          });
        })
        .fail(xhr => {
          this.setState({ error: xhr.responseJSON });
        });
    });
  }

  get customStyles() {
    return {
      content: {
        padding: "5px",
        width: "40%",
        height: "190px",
        top: "40%",
        left: "50%",
        right: "auto",
        bottom: "auto",
        marginRight: "-50%",
        transform: "translate(-50%, -50%)"
      }
    };
  }

  openModal = () => this.setState({ modalIsOpen: true });

  closeModal = () => this.setState({ modalIsOpen: false });

  killJob = jobId => {
    fetch(`/api/v3/jobs/${jobId}`, null, "DELETE")
      .done(() => {
        this.setState({ killJobRequestSent: true });
        this.closeModal();
      })
      .fail(xhr => {
        this.setState({
          modalIsOpen: false,
          killJobRequestSent: true,
          killRequestError: xhr.responseJSON
        });
      });
  };

  render() {
    return (
      <tr>
        <td colSpan="12">
          <i className="fa fa-sort-desc" aria-hidden="true" />
          <div className="job-detail-row">
            <table className="table job-detail-table">
              <tbody>
                <tr>
                  <td className="col-xs-2 align-right">Job Id:</td>
                  <td>{this.state.job.id}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Version:</td>
                  <td>{this.state.job.version}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Description:</td>
                  <td>{this.state.job.description}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Tags:</td>
                  <td>
                    <ul>
                      {this.state.job.tags.map((tag, index) => (
                        <li key={index}>{tag}</li>
                      ))}
                    </ul>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Status Msg:</td>
                  <td>{this.state.job.statusMsg}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Created:</td>
                  <td>{momentFormat(this.state.job.created)}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Updated:</td>
                  <td>{momentFormat(this.state.job.updated)}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Archive Location:</td>
                  <td>{this.state.job.archiveLocation}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Application Ids:</td>
                  <td>
                    <ul>
                      {this.state.applications.map((application, index) => (
                        <li key={index}>
                          <Link
                            to={`applications?name=${application.name}&rowId=${
                              application.id
                            }`}
                          >
                            {application.id}
                          </Link>
                        </li>
                      ))}
                    </ul>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Cluster Id:</td>
                  <td>
                    <Link
                      to={`clusters?name=${this.state.job.clusterName}&rowId=${
                        this.state.cluster.id
                      }`}
                    >
                      {this.state.cluster.id}
                    </Link>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Command Id:</td>
                  <td>
                    <Link
                      to={`commands?name=${this.state.job.commandName}&rowId=${
                        this.state.command.id
                      }`}
                    >
                      {this.state.command.id}
                    </Link>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Command Args:</td>
                  <td>{this.state.job.commandArgs}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Grouping:</td>
                  <td>{this.state.job.grouping}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Grouping Instance:</td>
                  <td>{this.state.job.groupingInstance}</td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Details:</td>
                  <td>
                    <ul>
                      <li>
                        <a href={this.state.job._links.cluster.href}>Cluster</a>
                      </li>
                      <li>
                        <a href={this.state.job._links.command.href}>Command</a>
                      </li>
                      <li>
                        <a href={this.state.job._links.applications.href}>
                          Application
                        </a>
                      </li>
                    </ul>
                  </td>
                </tr>
                <tr>
                  <td className="col-xs-2 align-right">Links:</td>
                  <td>
                    <ul>
                      <li>
                        <a href={this.state.job._links.self.href}>Json</a>
                      </li>
                      <li>
                        <a href={this.state.job._links.request.href}>Request</a>
                      </li>
                      <li>
                        <a href={this.state.job._links.execution.href}>
                          Execution
                        </a>
                      </li>
                      <li>
                        <a href={this.state.job._links.status.href}>Status </a>
                      </li>
                    </ul>
                  </td>
                </tr>
                {(this.state.job.status === "RUNNING" ||
                  this.state.job.status === "INIT") &&
                !this.state.killJobRequestSent ? (
                  <tr>
                    <td>
                      <button
                        type="button"
                        className="btn btn-danger"
                        onClick={this.openModal}
                      >
                        Send Kill Request
                      </button>
                      <Modal
                        isOpen={this.state.modalIsOpen}
                        onRequestClose={this.closeModal}
                        style={this.customStyles}
                      >
                        <div>
                          <div className="modal-header">
                            <h4 className="modal-title" id="alert-modal-label">
                              Confirm Kill Request
                            </h4>
                          </div>
                          <div className="modal-body">
                            <div>This cannot be undone.</div>
                          </div>
                          <div className="modal-footer">
                            <button
                              type="button"
                              className="btn btn-primary"
                              onClick={() => this.killJob(this.state.job.id)}
                            >
                              OK
                            </button>
                            <button
                              type="button"
                              className="btn btn-default"
                              onClick={this.closeModal}
                            >
                              Cancel
                            </button>
                          </div>
                        </div>
                      </Modal>
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
            {this.state.killJobRequestSent && !this.state.killRequestError ? (
              <small>
                *Request accepted. Please refresh the page in a few seconds to
                see the status change.
              </small>
            ) : null}
            {this.state.killRequestError ? (
              <div>
                <div>
                  <small>
                    *Request failed. Please refresh the page and try again.
                  </small>
                </div>
                <div>
                  <small>
                    <code>{this.state.killRequestError.message}.</code>
                  </small>
                </div>
              </div>
            ) : null}
            {this.state.error ? (
              <div>
                <div>
                  <span>Failed to load Command details:</span>
                </div>
                <Error error={this.state.error} />
              </div>
            ) : null}
          </div>
        </td>
      </tr>
    );
  }
}
