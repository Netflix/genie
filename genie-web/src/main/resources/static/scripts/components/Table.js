import T from "prop-types";
import React from "react";

const Table = props =>
  <div className="table-responsive">
    <table className="table">
      {props.children}
    </table>
  </div>;

Table.propTypes = { children: T.array.isRequired };

export default Table;
