import T from "prop-types";
import React from "react";
import { Link } from "react-router";
import { momentFormat } from "../utils";

import CopyToClipboard from "react-copy-to-clipboard";

export default class TableRow extends React.Component {
  static propTypes = {
    toggleRowDetails: T.func.isRequired,
    timeZone: T.string.isRequired,
    row: T.shape({
      id: T.string,
      name: T.string,
      user: T.string,
      status: T.string,
      version: T.string,
      tags: T.arrayOf(T.string),
      created: T.string,
      updated: T.string
    })
  };

  constructor(props) {
    super(props);
    this.state = { copied: false };
  }

  get url() {
    const { protocol, hostname, pathname } = window.location;
    return `${protocol}//${hostname}${pathname}`;
  }

  render() {
    return (
      <tr className="job-table-row">
        <td
          className="link"
          onClick={() => this.props.toggleRowDetails(this.props.row.id)}
        >
          {this.props.row.id}
        </td>
        <td onClick={() => this.props.toggleRowDetails(this.props.row.id)}>
          {this.props.row.name}
        </td>
        <td
          onMouseMove={() => {
            this.setState({ copied: false });
          }}
        >
          <CopyToClipboard
            onCopy={() => this.setState({ copied: true })}
            text={`${this.url}?name=${this.props.row.name}&rowId=${
              this.props.row.id
            }`}
          >
            <button className="btn btn-default btn-xs">
              <i className="fa fa-share-alt" aria-hidden="true" />
            </button>
          </CopyToClipboard>
          {this.state.copied ? (
            <p className="text-muted copied-text">Copied</p>
          ) : null}
        </td>
        <td onClick={() => this.props.toggleRowDetails(this.props.row.id)}>
          {this.props.row.user}
        </td>
        <td onClick={() => this.props.toggleRowDetails(this.props.row.id)}>
          {this.props.row.status}
        </td>
        <td onClick={() => this.props.toggleRowDetails(this.props.row.id)}>
          {this.props.row.version}
        </td>
        <td onClick={() => this.props.toggleRowDetails(this.props.row.id)}>
          <ul>
            {this.props.row.tags.map((tag, index) => (
              <li key={index}>
                <Link to={`${this.url}?tag=${tag}`}>{tag}</Link>
              </li>
            ))}
          </ul>
        </td>
        <td
          onClick={() => this.props.toggleRowDetails(this.props.row.id)}
          className="col-xs-2"
        >
          {momentFormat(this.props.row.created, this.props.timeZone)}
        </td>
        <td
          onClick={() => this.props.toggleRowDetails(this.props.row.id)}
          className="col-xs-2"
        >
          {momentFormat(this.props.row.updated, this.props.timeZone)}
        </td>
      </tr>
    );
  }
}
