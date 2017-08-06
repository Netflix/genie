import T from "prop-types";
import React from "react";

const TableBody = props => {
  const tableRows = props.rows.map((row, index) =>
    <props.rowType
      key={index}
      row={row}
      toggleRowDetails={props.toggleRowDetails}
    />
  );

  if (props.rowId) {
    const filteredRow = props.rows.find(row => row.id === props.rowId);
    const index = props.rows.findIndex(row => row.id === props.rowId);

    tableRows.splice(
      index + 1,
      0,
      <props.detailsTable
        key={`rowIndex-${index}`}
        row={filteredRow}
        toggleRowDetails={props.toggleRowDetails}
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
  rows: T.arrayOf(T.object).isRequired,
  rowId: T.string,
  rowType: T.func.isRequired,
  detailsTable: T.func.isRequired,
  toggleRowDetails: T.func.isRequired
};

export default TableBody;
