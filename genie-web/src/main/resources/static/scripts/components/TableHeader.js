import React, { PropTypes } from 'react';

const TableHeader = (props) =>
  <thead>
    <tr>
      {props.headers.map((header, index) =>
        <td key={index}><span>{header}</span></td>
      )}
    </tr>
  </thead>;

TableHeader.propTypes = {
  headers: PropTypes.arrayOf(PropTypes.string),
};

export default TableHeader;
