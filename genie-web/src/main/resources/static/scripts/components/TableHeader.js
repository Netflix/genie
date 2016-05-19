import React from 'react';
import { render } from 'react-dom';

const TableHeader = (props) =>
  <thead>
    <tr>
      {props.headers.map((header, index) =>
        <td key={index}><span>{header}</span></td>
      )}
    </tr>
  </thead>;

export default TableHeader;
