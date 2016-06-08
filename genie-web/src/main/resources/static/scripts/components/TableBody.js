import React, { PropTypes as T } from 'react';

import TableRow from './TableRow';

const TableBody = (props) => {
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
      <props.detailsTable
        row={filteredRow}
        key={`rowIndex-${index}`}
        hideDetails={props.hideDetails}
      />
    );
  }

  return (
    <tbody>
      {tableRows}
    </tbody>
  );
};

TableBody.propTypes = {
  rows        : T.arrayOf(T.object).isRequired,
  rowId       : T.string,
  hideDetails : T.func.isRequired,
  setRowId    : T.func.isRequired,
};

export default TableBody;
