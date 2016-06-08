import React, { PropTypes as T } from 'react';
import { Link } from 'react-router';

import { momentFormat } from '../utils';
import moment from 'moment';

import JobDetails from './JobDetails';

const TableRow = (props) =>
  <tr>
    <td>
      <a
        href="javascript:void(0)"
        value={props.row.id}
        onClick={() => props.setRowId(props.row.id)}
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
  row: T.shape({
    id          : T.string,
    name        : T.string,
    user        : T.string,
    status      : T.string,
    clusterName : T.string,
    started     : T.string,
    finished    : T.string,
    runtime     : T.string,
  }),
};

const JobTableBody = (props) => {
  const tableRows = props.rows.map((row, index) => {
    return (
      <TableRow
        key={index}
        row={row}
        setRowId={props.setRowId}
      />
    );
  });

  if (props.rowId) {
    const filteredRow = props.rows.find((row) => row.id === props.rowId);
    const index = props.rows.findIndex((row) => row.id === props.rowId);

    tableRows.splice(
      index + 1,
      0,
      <JobDetails
        row={filteredRow}
        key={`rowIndex-${index}`}
        hideDetails={props.hideDetails}
        killJob={props.killJob}
        killJobRequestSent={props.killJobRequestSent}
      />
    );
  }

  return (
    <tbody>
      {tableRows}
    </tbody>
  );
};

JobTableBody.propTypes = {
  rows        : T.arrayOf(T.object).isRequired,
  rowId       : T.string,
  hideDetails : T.func.isRequired,
  setRowId    : T.func.isRequired,
  killJob     : T.func.isRequired,
};

export default JobTableBody;

