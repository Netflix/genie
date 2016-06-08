import React, { PropTypes as T } from 'react';

const Table = (props) =>
  <div className="table-responsive">
    <table className="table">
      {props.header}
      {props.body}
    </table>
  </div>;

Table.propTypes = {
  header : T.object.isRequired,
  body   : T.object.isRequired,
};

export default Table;
