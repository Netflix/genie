import React, { PropTypes } from 'react';
import { Link } from 'react-router';

import { momentFormat, fetch } from '../utils';
import moment from 'moment';

import TableBody from './TableBody';
import JobDetails from './JobDetails';

const TableRow = (props) =>
  <tr>
    <td>
      <a
        target="_blank"
        href="javascript:void(0)"
        value={props.row.id}
        onClick={() => props.setShowDetails(props.row.id)}
      >
        {props.row.id}
      </a>
    </td>
    <td>{props.row.name}</td>
    <td>{props.row.user}</td>
    <td>{props.row.status}</td>
    <td>{props.row.clusterName}</td>
    <td>
      <Link
        target="_blank"
        to={`/output/${props.row.id}/output`}
      >
        <i className="fa fa-lg fa-folder" aria-hidden="true"></i>
      </Link>
    </td>

    <td className="col-xs-2">{props.row.started ? momentFormat(props.row.started) : 'NA'}</td>
    <td className="col-xs-2">{props.row.finished ? momentFormat(props.row.finished) : 'NA'}</td>
    <td className="col-xs-1">{props.row.started ? moment.duration(props.row.runtime).humanize() : 'NA'}</td>
  </tr>;

TableRow.propTypes = {
  row: PropTypes.shape({
    id          : PropTypes.string,
    name        : PropTypes.string,
    user        : PropTypes.string,
    status      : PropTypes.string,
    clusterName : PropTypes.string,
    started     : PropTypes.string,
    finished    : PropTypes.string,
    runtime     : PropTypes.string,
  }),
};

export default class JobTableBody extends TableBody {

  killJob = (jobId) => {
    fetch(`/api/v3/jobs/${jobId}`, null, 'DELETE')
      .done(() => {
        const { query, pathname } = this.context.location;
        this.context.router.push({
          query,
          pathname,
        });
      });
  }

  render() {
    let tableRows = this.construct(TableRow);

    if (this.state.showDetails) {
      tableRows.splice(
        this.state.index + 1,
        0,
        <JobDetails
          jobUrl={this.state.row._links.self.href}
          key="`${jobId}-details`"
          hideDetails={this.hideDetails}
          killJob={this.killJob}
        />
      );
    }

    return (
      <tbody>
        {tableRows}
      </tbody>
    );
  }
}

