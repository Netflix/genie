import React from 'react';
import { Link } from 'react-router';
import { render } from 'react-dom';

import moment from 'moment';

import Pagination from './Pagination';
import TableHeader from './TableHeader';
import JobDetailTable from './JobDetailTable';

const JobSearchResult = (props) =>
  <div className={props.showSearchForm ? "col-md-10" : "col-md-12"}>
    <table className="table">
      <TableHeader headers={props.headers} />
      <TableBody jobs={props.jobs} />
    </table>
    <Pagination page={props.page} links={props.links} />
  </div>;

export default JobSearchResult;

class TableBody extends React.Component {
  constructor(props) {
    super(props);
    this.state = this.defaultJobDetailsState();
  }

  componentWillReceiveProps(nextProps) {
    this.hideJobDetails();
  }

  defaultJobDetailsState() {
    return {
      jobUrl         : '',
      jobIndex       : -1,
      showJobDetails : false,
    }
  }

  hideJobDetails = () => {
    this.setState(this.defaultJobDetailsState());
  }

  showJobDetails = (jobId, jobIndex) => {
    const job = this.props.jobs.find((job) => job.id === jobId);
    this.setState({
      showJobDetails : true,
      jobUrl         : job._links.self.href,
      jobIndex       : jobIndex,
    });
  }

  render() {
    let tableRows = this.props.jobs.map((job, index) => {
      return (
        <TableRow
          key={job.id}
          row={job}
          showJobDetails={this.showJobDetails}
          rowIndex={index}
        />
      );
    });

    if (this.state.showJobDetails) {
      tableRows.splice(
        this.state.jobIndex + 1,
        0,
        <JobDetailTable
          url={this.state.jobUrl}
          key="`${jobId}-details`"
          hideJobDetails={this.hideJobDetails}
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
          onClick={() => props.showJobDetails(props.row.id, props.rowIndex)}>
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
        to={`/genie-jobs/${props.row.id}/output`}>
          <i className="fa fa-lg fa-folder" aria-hidden="true"></i>
      </Link>
    </td>
    <td className="col-xs-1">{props.row.started}</td>
    <td className="col-xs-1">{props.row.finished}</td>
    <td className="col-xs-1">{moment.duration(props.row.runtime).humanize()}</td>
  </tr>;

