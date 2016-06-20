import React, { PropTypes as T } from 'react';

const Table = (props) =>
  <div className="table-responsive">
    <table className="table">
      {props.children}
    </table>
  </div>;

Table.propTypes = {
  children : T.array.isRequired,
};

export default Table;
