import React from 'react';
import { Link } from 'react-router';
import { render } from 'react-dom';

import { momentFormat } from '../utils';
import moment from 'moment';

import TableBody from './TableBody';
import TableHeader from './TableHeader';
import Pagination from './Pagination';
import JobDetails from './JobDetails';

export default class JobTableBody extends TableBody {
  constructor(props) {
    super(props);
  }

  killJob = (jobId) => {
    console.log("Kill ->" + jobId);
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

const TableRow = (props) =>
  <tr>
    <td>
      <a target="_blank" href="javascript:void(0)" value={props.row.id}
          onClick={() => props.setShowDetails(props.row.id)}>
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
        to={`/output/${props.row.id}/output`}>
          <i className="fa fa-lg fa-folder" aria-hidden="true"></i>
      </Link>
    </td>

    <td className="col-xs-2">{momentFormat(props.row.started)}</td>
    <td className="col-xs-2">{momentFormat(props.row.finished)}</td>
    <td className="col-xs-1">{moment.duration(props.row.runtime).humanize()}</td>
  </tr>;

